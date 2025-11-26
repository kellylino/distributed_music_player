# message_handler.py
import time
import threading

from common import send_json_on_sock, send_json_to_addr

class MessageHandler:
    def __init__(self, node_id, network_manager, election_module, playback_module):
        self.node_id = node_id
        self.network = network_manager
        self.election = election_module
        self.playback = playback_module

    def handle_message(self, msg, conn):
        m = msg.get("type")

        # Handle election messages - separate COORDINATOR for special handling
        if m == "COORDINATOR":
            self._handle_coordinator_message(msg, conn)
            return
        elif m == "ELECTION":
            def send_response(response_msg):
                send_json_on_sock(conn, response_msg)
            self.election.handle_election_message(msg, send_response)
            return

        # Handle network messages
        if m == "HELLO":
            self._handle_hello(msg, conn)
        elif m == "DISCOVERY_REQUEST":
            self._handle_discovery_request(msg, conn)
        elif m == "DISCOVERY_RESPONSE":
            self._handle_discovery_response(msg)
        elif m == "HEARTBEAT":
            self._handle_heartbeat(conn)
        elif m == "RECONNECT_REQUEST":
            self._handle_reconnect_request(msg, conn)

        # Handle playback messages
        elif m == "PLAY_REQUEST":
            self._handle_play_request(msg)
        elif m == "PAUSE_REQUEST":
            self._handle_pause_request(msg)
        elif m == "RESUME_REQUEST":
            self._handle_resume_request(msg)
        elif m == "STOP_REQUEST":
            self._handle_stop_request(msg)

        # Handle other messages
        elif m == "LEADER_DISCOVERY":
            self._handle_leader_discovery(msg, conn)
        elif m == "CLOCK_SYNC_REQUEST":
            self._handle_clock_sync_request(msg, conn)

    def _handle_hello(self, msg, conn):
        sender_id = msg.get("sender_id")
        sender_host = msg.get("host")
        sender_port = msg.get("port")
        is_leader = msg.get("is_leader", False)
        leader_id = msg.get("leader_id")

        # Always update peer information when we receive HELLO
        if sender_host and sender_port:
            self.network.update_peer_info(sender_id, sender_host, sender_port, is_leader)
            print(f"[HELLO] Updated peer info for {sender_id} at {sender_host}:{sender_port}")

        send_json_on_sock(conn, {
            "type": "HELLO",
            "sender_id": self.node_id,
            "host": self.network.host,
            "port": self.network.port,
            "is_leader": self.election.is_leader,
            "leader_id": self.election.leader_id
        })

        # Update leader information if the sender is a leader
        if is_leader and leader_id:
            self.election.leader_id = leader_id
            print(f"[HELLO] Updated leader to {leader_id} from {sender_id}")

    def _handle_discovery_request(self, msg, conn):
        peers = self.network.get_peers()
        plist = [{"peer_id": self.node_id, "host": self.network.host, "port": self.network.port, "is_leader": self.election.is_leader}]

        for pid, (h, p) in peers.items():
            plist.append({"peer_id": pid, "host": h, "port": p, "is_leader": (pid == self.election.leader_id)})

        send_json_on_sock(conn, {
            "type": "DISCOVERY_RESPONSE",
            "sender_id": self.node_id,
            "peers": plist
        })

    def _handle_reconnect_request(self, msg, conn):
        """Handle peer reconnection requests"""
        peer_id = msg.get("sender_id")  # Change from "peer_id" to "sender_id"
        peer_host = msg.get("host")
        peer_port = msg.get("port")

        print(f"[RECONNECT] Received reconnect request from {peer_id} at {peer_host}:{peer_port}")

        # Add/update peer info
        if peer_host and peer_port:  # Add validation
            self.network.update_peer_info(peer_id, peer_host, peer_port, False)
            print(f"[RECONNECT] Updated peer info for {peer_id}")

        # Send acknowledgment
        send_json_on_sock(conn, {
            "type": "RECONNECT_ACK",
            "sender_id": self.node_id,
            "status": "accepted"
        })

    def _handle_discovery_response(self, msg):
        for e in msg.get("peers", []):
            pid, h, p = e["peer_id"], e["host"], e["port"]
            is_leader = e.get("is_leader", False)

            if pid == self.node_id:
                continue

            self.network.update_peer_info(pid, h, p, is_leader)

            if is_leader and self.election.leader_id != pid:
                self.election.leader_id = pid

    def _handle_heartbeat(self, conn):
        send_json_on_sock(conn, {"type": "HEARTBEAT_ACK", "sender_id": self.node_id})

    def _handle_play_request(self, msg):
        track = msg.get("track")
        start_time = msg.get("start_time")
        leader_id = msg.get("leader_id")

        # Calculate delay and schedule playback
        delay = start_time - time.time()
        self.playback.prepare_and_schedule_play(track, delay)

    def _handle_pause_request(self, msg):
        pause_time = msg.get("pause_time")
        delay = pause_time - time.time()
        self.playback.prepare_and_schedule_pause(delay)

    def _handle_resume_request(self, msg):
        resume_time = msg.get("resume_time")
        delay = resume_time - time.time()
        self.playback.prepare_and_schedule_resume(delay)

    def _handle_stop_request(self, msg):
        stop_time = msg.get("stop_time")

        # Calculate delay and schedule stop
        delay = stop_time - time.time()
        self.playback.prepare_and_schedule_stop(delay)

    def _handle_leader_discovery(self, msg, conn):
        requested_leader_id = msg.get("leader_id")
        if requested_leader_id == self.election.leader_id:
            leader_host, leader_port = self.network.get_peer_address(self.election.leader_id)
            if leader_host and leader_port:
                send_json_on_sock(conn, {
                    "type": "LEADER_INFO",
                    "sender_id": self.node_id,
                    "leader_id": self.election.leader_id,
                    "host": leader_host,
                    "port": leader_port
                })

    def _handle_clock_sync_request(self, msg, conn):
        follower_time = time.time()
        send_json_on_sock(conn, {
            "type": "CLOCK_SYNC_RESPONSE",
            "sender_id": self.node_id,
            "follower_time": follower_time
        })

    def _handle_coordinator_message(self, msg, conn):
        """Handle COORDINATOR message from new leader"""
        new_leader_id = msg.get("leader_id")
        leader_host = msg.get("host")
        leader_port = msg.get("port")

        print(f"[ELECTION] Received COORDINATOR from new leader: {new_leader_id}")

        self.election.leader_id = new_leader_id
        self.election.is_leader = (new_leader_id == self.node_id)

        if leader_host and leader_port:
            self.network.update_peer_info(new_leader_id, leader_host, leader_port, True)
            print(f"[ELECTION] Added new leader {new_leader_id} to peer list at {leader_host}:{leader_port}")

            if new_leader_id != self.node_id:
                try:
                    self.network.connect_to_peer(leader_host, leader_port)
                    print(f"[ELECTION] Connected to new leader {new_leader_id}")
                except Exception as e:
                    print(f"[ELECTION] Failed to connect to new leader: {e}")
    def broadcast_reconnect_request(self):
        """Broadcast reconnect request to all known peers"""
        peers = self.network.get_peers()

        for pid, (host, port) in peers.items():
            if pid == self.node_id:
                continue

            try:
                send_json_to_addr(host, port, {
                    "type": "RECONNECT_REQUEST",
                    "sender_id": self.node_id,
                    "host": self.network.host,
                    "port": self.network.port
                })
                print(f"[RECONNECT] Sent reconnect request to {pid}")
            except Exception as e:
                print(f"[RECONNECT] Failed to send to {pid}: {e}")