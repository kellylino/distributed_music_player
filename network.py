# network.py
"""
Network Manager Module - Handles Peer-to-Peer Communication
This module manages all network connections, message broadcasting,
peer discovery, and connection maintenance.
"""

import time
import random
import socket
import threading

from common import send_json_on_sock, send_json_to_addr, recv_json_from_sock

# Network configuration constants
HEARTBEAT_INTERVAL = 1.0 # Seconds between heartbeats
MONITOR_INTERVAL = 2.0 # Seconds between peer monitoring checks
PEER_TIMEOUT = 4.0 # Seconds before marking peer as dead

def now():
    """Helper function to get current timestamp."""
    return time.time()

class NetworkManager:
    """
    Manages all network communication for a node.

    Responsibilities:
    - Maintain connections to other peers
    - Send and receive messages
    - Handle peer discovery and removal
    - Broadcast messages to all peers
    - Monitor peer health with heartbeats
    """

    def __init__(self, host, port, node_id, on_message_callback, on_peer_update_callback, on_leader_update_callback=None):
        """
        Initialize network manager.

        Args:
            host (str): This node's hostname/IP
            port (int): This node's port
            node_id (str): This node's unique ID
            on_message_callback (function): Callback for incoming messages
            on_peer_update_callback (function): Callback for peer status changes
            on_leader_update_callback (function): Callback for leader updates
        """

        self.host = host
        self.port = port
        self.node_id = node_id
        self.on_message_callback = on_message_callback
        self.on_peer_update_callback = on_peer_update_callback
        self.on_leader_update_callback = on_leader_update_callback

        # Peer management data structures
        self.peers = {} # peer_id -> (host, port)
        self.last_seen = {} # peer_id -> timestamp of last communication
        self.running = True # Control flag
        self.lock = threading.Lock() # Protect shared data

        # Connection manager for persistent connections
        self.connection_manager = ConnectionManager()

    def start(self):
        """Start all network-related background threads."""
        threading.Thread(target=self._run_server, daemon=True).start() # Listen for connections
        threading.Thread(target=self._heartbeat_loop, daemon=True).start() # Send heartbeats
        threading.Thread(target=self._monitor_loop, daemon=True).start() # Monitor peer health
        threading.Thread(target=self._network_sync_loop, daemon=True).start()  # Periodic sync

    def connect_to_peer(self, host, port):
        """
        Connect to a new peer and exchange information.

        Args:
            host (str): Peer's hostname/IP
            port (int): Peer's port

        Returns:
            tuple: (peer_id, response) or (None, None) on failure
        """

        try:
            # Send HELLO message to initiate connection
            resp = send_json_to_addr(host, port, {
                "type": "HELLO",
                "sender_id": self.node_id,
                "host": self.host,
                "port": self.port,
            })

            if resp and resp.get("type") == "HELLO":
                pid = resp.get("sender_id")
                print(f"[CONNECT_DEBUG] Successfully connected to {pid} at {host}:{port}")

                with self.lock:
                    self.peers[pid] = (host, port)
                    self.last_seen[pid] = now()

                # Request discovery to learn about other peers
                send_json_to_addr(host, port, {
                    "type": "DISCOVERY_REQUEST",
                    "sender_id": self.node_id
                })
                return pid, resp

        except Exception as e:
            print(f"[CONNECT_DEBUG] Failed to connect to {host}:{port}: {e}")
        return None, None

    def broadcast_message(self, message_type, data, leader_id):
        """
        Broadcast message to all peers including self.

        Uses parallel sending for efficiency with configurable timeouts.

        Args:
            message_type (str): Type of message
            data (dict): Message payload
            leader_id (str): ID of leader (for verification)
        """

        # Set timeout based on message type
        if message_type in ["PLAY_REQUEST", "PAUSE_REQUEST", "RESUME_REQUEST", "STOP_REQUEST"]:
            timeout = 0.05 # Shorter timeout for time-critical messages
        else:
            timeout = 0.1

        # Get copy of peers to avoid locking during sending
        with self.lock:
            peers_copy = dict(self.peers)
            # Include self in recipients for local processing
            all_recipients = list(peers_copy.items()) + [(self.node_id, (self.host, self.port))]

        # Use threading for parallel sending
        threads = []
        for pid, (h, p) in all_recipients:
            thread = threading.Thread(
                target=self._send_to_peer,
                args=(h, p, pid, message_type, data, leader_id),
                daemon=True
            )
            thread.start()
            threads.append(thread)

        # Wait for completion
        for thread in threads:
            thread.join(timeout=timeout)

    def _send_to_peer(self, host, port, peer_id, message_type, data, leader_id):
        """
        Send message to specific peer.

        Args:
            host (str): Peer's host
            port (int): Peer's port
            peer_id (str): Peer's ID
            message_type (str): Message type
            data (dict): Message data
            leader_id (str): Leader ID

        Returns:
            bool: True if sent successfully
        """

        message = {
            "type": message_type,
            "sender_id": self.node_id,
            "leader_id": leader_id,
            **data
        }

        if peer_id == self.node_id:
            # Special handling for self messages
            class MockConn:
                def close(self): pass
            # Process message locally in separate thread
            threading.Thread(
                target=self.on_message_callback,
                args=(message, MockConn()),
                daemon=True
            ).start()
            return True
        else:
            # Send to remote peer using connection manager
            success = self.connection_manager.send_message(host, port, peer_id, message)
            if not success:
                print(f"[SEND] Failed to send {message_type} to {peer_id}")

    def _run_server(self):
        """Run TCP server to accept incoming connections."""

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen()

        while self.running:
            try:
                conn, addr = sock.accept()
                # Handle each connection in separate thread
                threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()
            except Exception:
                pass

    def _handle_conn(self, conn):
        """
        Handle incoming connection.

        Reads JSON messages until connection closes or error occurs.

        Args:
            conn (socket): Accepted connection
        """

        while True:
            msg = recv_json_from_sock(conn)
            if msg is None:
                break
            self._update_last_seen(msg)
            self.on_message_callback(msg, conn)
        try:
            conn.close()
        except:
            pass

    def _update_last_seen(self, msg):
        """Update last_seen timestamp for message sender."""

        sid = msg.get("sender_id")
        if sid:
            with self.lock:
                self.last_seen[sid] = now()
                if "host" in msg and "port" in msg:
                    self.peers[sid] = (msg["host"], msg["port"])

    def _heartbeat_loop(self):
        """Periodically send heartbeat messages to all peers."""

        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                peers_copy = dict(self.peers)

            for pid, (h, p) in peers_copy.items():
                try:
                    resp = send_json_to_addr(h, p, {"type": "HEARTBEAT", "sender_id": self.node_id})
                    if resp and resp.get("type") == "HEARTBEAT_ACK":
                        with self.lock:
                            self.last_seen[pid] = now()
                except Exception as e:
                    print(f"[HEARTBEAT] {pid} unreachable: {e}")

    def _monitor_loop(self):
        """Monitor peer health and remove dead peers."""

        while self.running:
            time.sleep(MONITOR_INTERVAL)
            nowt = now()
            removed = []

            with self.lock:
                for pid, last in list(self.last_seen.items()):
                    if pid == self.node_id:
                        continue
                    if nowt - last > PEER_TIMEOUT: # Remove peers that haven't been seen for timeout period
                        removed.append(pid)
                        if pid in self.peers: del self.peers[pid]
                        if pid in self.last_seen: del self.last_seen[pid]

            # Notify about removed peers
            for pid in removed:
                self.on_peer_update_callback("removed", pid)

    def _network_sync_loop(self):
        """Periodically sync network topology with random peer."""

        while self.running:
            time.sleep(5.0) # Sync every 5 seconds
            with self.lock:
                if not self.peers:
                    continue

                # Choose random peer for synchronization
                peers_list = list(self.peers.items())
                if peers_list:
                    random_peer = random.choice(peers_list)
                    pid, (h, p) = random_peer

            try:
                # Request discovery from random peer
                response = send_json_to_addr(h, p, {
                    "type": "DISCOVERY_REQUEST",
                    "sender_id": self.node_id
                })

                if response and response.get("type") == "DISCOVERY_RESPONSE":
                    # Process discovery response
                    class MockConn:
                        def close(self): pass
                        def sendall(self, data): pass
                    self.on_message_callback(response, MockConn())

            except Exception as e:
                print(f"[SYNC_DEBUG] Failed to sync with {pid}: {e}")

    def get_peer_address(self, peer_id):
        """
        Get address of a peer.

        Args:
            peer_id (str): Peer's ID

        Returns:
            tuple: (host, port) or (None, None) if not found
        """

        with self.lock:
            return self.peers.get(peer_id, (None, None))

    def get_peers(self):
        """Get copy of all peers."""

        with self.lock:
            return dict(self.peers)

    def update_peer_info(self, peer_id, host, port, is_leader):
        """
        Update or add peer information.

        Args:
            peer_id (str): Peer's ID
            host (str): Peer's host
            port (int): Peer's port
            is_leader (bool): Whether peer is leader
        """

        with self.lock:
            self.peers[peer_id] = (host, port)
            self.last_seen[peer_id] = now()

    def __del__(self):
        """Cleanup when network manager is destroyed."""
        self.connection_manager.close_all()

class ConnectionManager:
    """
    Manages persistent connections to peers.

    Maintains connection pool to avoid reconnecting for each message.
    """

    def __init__(self):
        self.connections = {} # peer_id -> socket
        self.lock = threading.Lock()

    def get_connection(self, host, port, peer_id):
        """
        Create persistent connection to peer.

        Args:
            host (str): Peer's host
            port (int): Peer's port
            peer_id (str): Peer's ID

        Returns:
            socket: Connection socket or None on failure
        """

        with self.lock:
            # Check existing connection
            if peer_id in self.connections:
                sock = self.connections[peer_id]
                try:
                    # Verify connection is still alive
                    sock.getpeername()
                    return sock
                except:
                    # Connection is dead, remove it
                    del self.connections[peer_id]
                    try:
                        sock.close()
                    except:
                        pass

            # Create new connection with proper timeout
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

                # Set connection timeout
                sock.settimeout(1.0)
                sock.connect((host, port))

                # Set longer timeout for subsequent operations
                sock.settimeout(2.0)

                self.connections[peer_id] = sock
                return sock
            except Exception as e:
                print(f"[CONN] Failed to connect to {peer_id}: {e}")
                return None

    def send_message(self, host, port, peer_id, message):
        """
        Send message using persistent connection.

        Args:
            host (str): Peer's host
            port (int): Peer's port
            peer_id (str): Peer's ID
            message (dict): Message to send

        Returns:
            bool: True if sent successfully
        """

        sock = self.get_connection(host, port, peer_id)
        if sock is None:
            return False

        try:
            send_json_on_sock(sock, message)
            return True
        except Exception as e:
            print(f"[CONN] Failed to send to {peer_id}: {e}")
            # Remove broken connection
            with self.lock:
                if peer_id in self.connections:
                    del self.connections[peer_id]
            try:
                sock.close()
            except:
                pass
            return False

    def close_all(self):
        """Close all persistent connections."""

        with self.lock:
            for sock in self.connections.items():
                try:
                    sock.close()
                except:
                    pass
            self.connections.clear()