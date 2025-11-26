# election.py
import threading
import time

from common import send_json_to_addr

def now():
    return time.time()

class BullyElection:
    def __init__(self, node_id, peers, get_peer_address, is_leader=False, leader_id=None):
        self.node_id = node_id
        self.peers = peers  # This should be a reference to the node's peers dict
        self.get_peer_address = get_peer_address  # Function to get (host, port) for a peer_id
        self.is_leader = is_leader
        self.leader_id = leader_id or (node_id if is_leader else None)
        self.election_in_progress = False
        self.lock = threading.Lock()

    def start_election(self, on_new_leader_callback=None):
        """Start a Bully algorithm election"""
        with self.lock:
            if self.election_in_progress:
                print("[ELECTION] Election already in progress, skipping...")
                return
            self.election_in_progress = True

        print("[ELECTION] Starting Bully algorithm election...")

        # Get all known peers with higher IDs than me
        all_known_peers = list(self.peers.keys()) + [self.node_id]
        higher_peers = [pid for pid in all_known_peers if pid > self.node_id]

        print(f"[ELECTION] My ID: {self.node_id}, Higher peers: {higher_peers}")

        if not higher_peers:
            # I have the highest ID, I become the leader
            self._become_leader(on_new_leader_callback)
            return

        # Send election messages to all higher peers
        election_responses = []
        response_lock = threading.Lock()

        def send_election_to_peer(pid):
            try:
                host, port = self.get_peer_address(pid)
                if not host or not port:
                    return

                print(f"[ELECTION] Sending ELECTION to higher peer: {pid}")
                resp = send_json_to_addr(host, port, {
                    "type": "ELECTION",
                    "sender_id": self.node_id,
                    "candidate_id": self.node_id
                })
                if resp and resp.get("type") == "ELECTION_ANSWER":
                    with response_lock:
                        election_responses.append(pid)
                    print(f"[ELECTION] Received ANSWER from {pid}")
            except Exception as e:
                print(f"[ELECTION] Failed to contact {pid}: {e}")

        # Send election messages to all higher peers
        threads = []
        for pid in higher_peers:
            t = threading.Thread(target=send_election_to_peer, args=(pid,), daemon=True)
            t.start()
            threads.append(t)

        # Wait for responses (with timeout)
        for t in threads:
            t.join(timeout=1.0)

        # If no responses from higher peers, I become the leader
        if not election_responses:
            self._become_leader(on_new_leader_callback)
        else:
            # Wait for coordinator message from the new leader
            self.is_leader = False
            print(f"[ELECTION] Waiting for COORDINATOR message from new leader...")
            # Set a timeout for coordinator wait
            threading.Thread(
                target=self._wait_for_coordinator,
                args=(on_new_leader_callback,),
                daemon=True
            ).start()

        with self.lock:
            self.election_in_progress = False

    def _become_leader(self, on_new_leader_callback):
        """This node becomes the new leader"""
        with self.lock:
            self.is_leader = True
            self.leader_id = self.node_id

        print(f"[ELECTION] I am the new leader: {self.node_id}")

        # Callback to notify the main node
        if on_new_leader_callback:
            on_new_leader_callback(self.node_id)

    def _wait_for_coordinator(self, on_new_leader_callback, timeout=2.0):
        """Wait for coordinator message with timeout"""
        start_time = now()

        while now() - start_time < timeout:
            if self.leader_id and self.leader_id != self.node_id:
                print(f"[ELECTION] New leader established: {self.leader_id}")
                if on_new_leader_callback:
                    on_new_leader_callback(self.leader_id)
                return
            time.sleep(0.5)

        if not self.leader_id or self.leader_id == self.node_id:
            print(f"[ELECTION] Coordinator timeout, restarting election")
            self.start_election(on_new_leader_callback)

    def handle_election_message(self, msg, send_response_callback):
        """Handle incoming election-related messages"""
        msg_type = msg.get("type")
        sender_id = msg.get("sender_id")

        if msg_type == "ELECTION":
            # Received election message from a peer with lower ID
            candidate_id = msg.get("candidate_id")
            print(f"[ELECTION] Received ELECTION from {candidate_id}")

            # Reply with answer
            send_response_callback({
                "type": "ELECTION_ANSWER",
                "sender_id": self.node_id
            })

            # Start my own election after a short delay
            threading.Timer(0.5, self.start_election).start()

        elif msg_type == "COORDINATOR":
            # Received coordinator announcement from new leader
            new_leader_id = msg.get("leader_id")
            leader_host = msg.get("host")
            leader_port = msg.get("port")

            print(f"[ELECTION] Received COORDINATOR from new leader: {new_leader_id}")

            with self.lock:
                self.leader_id = new_leader_id
                self.is_leader = (new_leader_id == self.node_id)

            # Update peers with leader info
            if new_leader_id not in self.peers and leader_host and leader_port:
                self.peers[new_leader_id] = (leader_host, leader_port)

    def announce_leadership(self, send_to_peer_callback):
        """Announce to all peers that I am the new leader"""
        print("[ANNOUNCE] Announcing myself as new leader to all peers")

        peers_copy = dict(self.peers)

        for pid in peers_copy:
            send_to_peer_callback(pid, {
                "type": "COORDINATOR",
                "sender_id": self.node_id,
                "leader_id": self.node_id,
                "host": None,  # These will be filled by the callback
                "port": None
            })

    def get_leader_info(self):
        """Get current leader information"""
        with self.lock:
            return {
                "is_leader": self.is_leader,
                "leader_id": self.leader_id,
                "node_id": self.node_id
            }

    def update_leader(self, new_leader_id):
        """Update leader information (e.g., from external source)"""
        with self.lock:
            old_leader = self.leader_id
            self.leader_id = new_leader_id
            self.is_leader = (new_leader_id == self.node_id)

            if old_leader != new_leader_id:
                print(f"[ELECTION] Leader updated from {old_leader} to {new_leader_id}")