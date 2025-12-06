# election.py
"""
Leader Election Module - Implements Bully Algorithm
This module handles leader election in a distributed system where
nodes with higher UUIDs have priority in becoming leaders.
"""

import time
import uuid
import threading

from common import send_json_to_addr

def now():
    """Helper function to get current timestamp."""
    return time.time()

class BullyElection:
    """
    Implements the Bully algorithm for leader election.

    The Bully algorithm:
    1. When a node detects leader failure, it starts an election
    2. It sends ELECTION messages to all nodes with higher IDs
    3. If no higher nodes respond, it declares itself leader
    4. If higher nodes respond, it waits for COORDINATOR message
    5. The highest ID among responding nodes becomes leader

    Election messages are sent in parallel with timeout for efficiency.
    """

    def __init__(self, node_id, peers, get_peer_address, is_leader=False, leader_id=None):
        """
        Initialize election module.

        Args:
            node_id (str): Unique identifier for this node
            peers (dict): Dictionary of peer_id -> (host, port)
            get_peer_address (function): Function to get address of a peer
            is_leader (bool): Whether this node starts as leader
            leader_id (str): ID of current leader if known
        """

        self.node_id = node_id
        self.peers = peers  # Shared reference to network peers
        self.get_peer_address = get_peer_address
        self.is_leader = is_leader
        self.leader_id = leader_id or (node_id if is_leader else None)
        self.election_in_progress = False # Prevent multiple concurrent elections
        self.lock = threading.Lock()  # Protect election state

    def start_election(self, on_new_leader_callback=None):
        """
        Start a new leader election using Bully algorithm.

        Args:
            on_new_leader_callback (function): Callback when new leader is elected
        """

        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True

        # Convert UUID strings for comparison
        my_uuid = uuid.UUID(self.node_id)
        higher_peers = []

        # Identify peers with higher UUIDs “will bully us”
        for peer_id in self.peers.keys():
            try:
                peer_uuid = uuid.UUID(peer_id)
                if peer_uuid > my_uuid:
                    higher_peers.append(peer_id)
            except ValueError:
                print(f"[ELECTION_WARNING] Invalid peer ID: {peer_id}")

         # No higher peers - I become the leader
        if not higher_peers:
            self._become_leader(on_new_leader_callback)
            return

        # Send election messages to all higher peers in parallel
        election_responses = []
        response_lock = threading.Lock()

        def send_election_to_peer(pid):
            """Send ELECTION message to a specific higher peer."""
            try:
                host, port = self.get_peer_address(pid)
                if not host or not port:
                    return

                resp = send_json_to_addr(host, port, {
                    "type": "ELECTION",
                    "sender_id": self.node_id,
                    "candidate_id": self.node_id
                })
                if resp and resp.get("type") == "ELECTION_ANSWER":
                    with response_lock:
                        election_responses.append(pid)
            except Exception as e:
                print(f"[ELECTION] Failed to contact {pid}: {e}")

        # Start threads for parallel message sending
        threads = []
        for pid in higher_peers:
            t = threading.Thread(target=send_election_to_peer, args=(pid,), daemon=True)
            t.start()
            threads.append(t)

        # Wait for responses with timeout
        for t in threads:
            t.join(timeout=2.0)

        # If no responses from higher peers, I become the leader
        if not election_responses:
            self._become_leader(on_new_leader_callback)
        else:
            # Wait for coordinator message from the new leader
            self.is_leader = False
            threading.Thread(
                target=self._wait_for_coordinator,
                args=(on_new_leader_callback,),
                daemon=True
            ).start()

        with self.lock:
            self.election_in_progress = False

    def _become_leader(self, on_new_leader_callback):
        """
        This node becomes the new leader.

        Updates local state and notifies the system via callback.
        """

        with self.lock:
            self.is_leader = True
            self.leader_id = self.node_id

        # Notify node about new leadership
        if on_new_leader_callback:
            on_new_leader_callback(self.node_id)

    def _wait_for_coordinator(self, on_new_leader_callback, timeout=2.0):
        """
        Wait for COORDINATOR message from new leader.

        If timeout occurs without coordinator message, restart election.

        Args:
            on_new_leader_callback (function): Callback when leader is known
            timeout (float): Seconds to wait before timing out
        """

        start_time = now()

        while now() - start_time < timeout:
            if self.leader_id and self.leader_id != self.node_id:
                # New leader established
                if on_new_leader_callback:
                    on_new_leader_callback(self.leader_id)
                return
            time.sleep(0.5)

        # Timeout - no coordinator received, restart election
        if not self.leader_id or self.leader_id == self.node_id:
            self.start_election(on_new_leader_callback)

    def _handle_election_message(self, msg, send_response_callback):
        """
        Handle incoming election-related messages

        Called by message_handler when election messages are received.

        Args:
            msg (dict): The election message
            send_response_callback (function): Function to send response
        """

        msg_type = msg.get("type")

        # Received election message from a peer with lower ID
        if msg_type == "ELECTION":

            # Respond with ELECTION_ANSWER
            send_response_callback({
                "type": "ELECTION_ANSWER",
                "sender_id": self.node_id
            })

            # Start own election after a short delay
            # This ensures the highest ID node eventually becomes leader
            threading.Timer(0.1, self.start_election).start()

        elif msg_type == "COORDINATOR":
            # Received leader announcement
            new_leader_id = msg.get("leader_id")
            leader_host = msg.get("host")
            leader_port = msg.get("port")

            with self.lock:
                self.leader_id = new_leader_id
                self.is_leader = (new_leader_id == self.node_id)

            # Update peers with leader info
            if new_leader_id not in self.peers and leader_host and leader_port:
                self.peers[new_leader_id] = (leader_host, leader_port)

    def announce_leadership(self, send_to_peer_callback):
        """
        Announce this node as leader to all peers.

        Args:
            send_to_peer_callback (function): Function to send message to peer
        """

        # Create copy to avoid modification during iteration
        peers_copy = dict(self.peers)

        for pid in peers_copy:
            send_to_peer_callback(pid, {
                "type": "COORDINATOR",
                "sender_id": self.node_id,
                "leader_id": self.node_id,
                "host": None,  # Will be filled by node
                "port": None   # Will be filled by node
            })
