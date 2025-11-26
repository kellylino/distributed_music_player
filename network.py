# network.py
import socket
import threading
import time
import random

from common import send_json_to_addr, recv_json_from_sock

class NetworkManager:
    def __init__(self, host, port, node_id, on_message_callback, on_peer_update_callback):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.on_message_callback = on_message_callback
        self.on_peer_update_callback = on_peer_update_callback

        self.peers = {}
        self.last_seen = {}
        self.clock_offsets = {}
        self.running = True
        self.lock = threading.Lock()

    def start(self):
        threading.Thread(target=self._run_server, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        threading.Thread(target=self._network_sync_loop, daemon=True).start()

    def connect_to_peer(self, host, port):
        print(f"[CONNECT_DEBUG] Attempting to connect to {host}:{port}")
        try:
            resp = send_json_to_addr(host, port, {
                "type": "HELLO",
                "sender_id": self.node_id,
                "host": self.host,
                "port": self.port,
                "is_leader": False,  # Will be updated by caller
                "leader_id": None    # Will be updated by caller
            })

            if resp and resp.get("type") == "HELLO":
                pid = resp.get("sender_id")
                print(f"[CONNECT_DEBUG] Successfully connected to {pid} at {host}:{port}")

                with self.lock:
                    self.peers[pid] = (host, port)
                    self.last_seen[pid] = time.time()

                # Ask for discovery
                send_json_to_addr(host, port, {
                    "type": "DISCOVERY_REQUEST",
                    "sender_id": self.node_id
                })

                return pid, resp

        except Exception as e:
            print(f"[CONNECT_DEBUG] Failed to connect to {host}:{port}: {e}")
        return None, None

    def broadcast_message(self, message_type, data, leader_id=None):
        with self.lock:
            peers_copy = dict(self.peers)

        for pid, (h, p) in peers_copy.items():
            try:
                message = {
                    "type": message_type,
                    "sender_id": self.node_id,
                    "leader_id": leader_id or self.node_id,
                    **data
                }
                send_json_to_addr(h, p, message)
            except:
                pass

    def _run_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen()

        while self.running:
            try:
                conn, addr = sock.accept()
                threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()
            except Exception:
                pass

    def _handle_conn(self, conn):
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
        sid = msg.get("sender_id")
        if sid:
            with self.lock:
                self.last_seen[sid] = time.time()
                if "host" in msg and "port" in msg:
                    if sid not in self.peers:
                        self.peers[sid] = (msg["host"], msg["port"])

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(2.0)  # HEARTBEAT_INTERVAL
            with self.lock:
                peers_copy = dict(self.peers)

            for pid, (h, p) in peers_copy.items():
                try:
                    resp = send_json_to_addr(h, p, {"type": "HEARTBEAT", "sender_id": self.node_id})
                    if resp and resp.get("type") == "HEARTBEAT_ACK":
                        with self.lock:
                            self.last_seen[pid] = time.time()
                except:
                    pass

    def _monitor_loop(self):
        while self.running:
            time.sleep(1.0)  # MONITOR_INTERVAL
            nowt = time.time()
            removed = []

            with self.lock:
                for pid, last in list(self.last_seen.items()):
                    if pid == self.node_id:
                        continue
                    if nowt - last > 4.0:  # PEER_TIMEOUT
                        removed.append(pid)
                        if pid in self.peers: del self.peers[pid]
                        if pid in self.last_seen: del self.last_seen[pid]
                        if pid in self.clock_offsets: del self.clock_offsets[pid]

            for pid in removed:
                print("[TIMEOUT] removed", pid)
                self.on_peer_update_callback("removed", pid)

    def _network_sync_loop(self):
        while self.running:
            time.sleep(4.0)  # SYNC_INTERVAL
            with self.lock:
                if not self.peers:
                    continue
                peers_list = list(self.peers.items())
                if peers_list:
                    random_peer = random.choice(peers_list)
                    pid, (h, p) = random_peer

            try:
                response = send_json_to_addr(h, p, {
                    "type": "DISCOVERY_REQUEST",
                    "sender_id": self.node_id
                })

                if response and response.get("type") == "DISCOVERY_RESPONSE":
                    class MockConn:
                        def close(self): pass
                    self.on_message_callback(response, MockConn())

            except Exception as e:
                print(f"[SYNC_DEBUG] Failed to sync with {pid}: {e}")

    def get_peer_address(self, peer_id):
        with self.lock:
            return self.peers.get(peer_id, (None, None))

    def get_peers(self):
        with self.lock:
            return dict(self.peers)

    def update_peer_info(self, peer_id, host, port, is_leader=False):
        with self.lock:
            self.peers[peer_id] = (host, port)
            self.last_seen[peer_id] = time.time()