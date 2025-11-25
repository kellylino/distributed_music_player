# message_handler.py
import time
from common import send_json_on_sock

class MessageHandler:
    def __init__(self, node_id, network_manager, election_module, playback_module):
        self.node_id = node_id
        self.network = network_manager
        self.election = election_module
        self.playback = playback_module
        self.leader_id = None
        self.is_leader = False

    def handle_message(self, msg, conn):
        m = msg.get("type")
        sid = msg.get("sender_id")

        # Handle election messages
        if m in ["ELECTION", "COORDINATOR"]:
            def send_response(response_msg):
                send_json_on_sock(conn, response_msg)
            self.election.handle_election_message(msg, conn, send_response)
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

        # Handle playback messages
        elif m == "PLAY_REQUEST":
            self._handle_play_request(msg)
        elif m == "PAUSE_REQUEST":
            self._handle_pause_request(msg)
        elif m == "RESUME_REQUEST":
            self._handle_resume_request(msg)

        # Handle other messages
        elif m == "LEADER_DISCOVERY":
            self._handle_leader_discovery(msg, conn)
        elif m == "CLOCK_SYNC_REQUEST":
            self._handle_clock_sync_request(msg, conn)

    def _handle_hello(self, msg, conn):
        send_json_on_sock(conn, {
            "type": "HELLO",
            "sender_id": self.node_id,
            "host": self.network.host,
            "port": self.network.port,
            "is_leader": self.is_leader,
            "leader_id": self.leader_id
        })

        # Update leader information
        if msg.get("is_leader"):
            self.leader_id = msg.get("sender_id")

    def _handle_discovery_request(self, msg, conn):
        peers = self.network.get_peers()
        plist = [{"peer_id": self.node_id, "host": self.network.host, "port": self.network.port, "is_leader": self.is_leader}]

        for pid, (h, p) in peers.items():
            plist.append({"peer_id": pid, "host": h, "port": p, "is_leader": (pid == self.leader_id)})

        send_json_on_sock(conn, {
            "type": "DISCOVERY_RESPONSE",
            "sender_id": self.node_id,
            "peers": plist
        })

    def _handle_discovery_response(self, msg):
        for e in msg.get("peers", []):
            pid, h, p = e["peer_id"], e["host"], e["port"]
            is_leader = e.get("is_leader", False)

            if pid == self.node_id:
                continue

            self.network.update_peer_info(pid, h, p, is_leader)

            if is_leader and self.leader_id != pid:
                self.leader_id = pid

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

    def _handle_leader_discovery(self, msg, conn):
        requested_leader_id = msg.get("leader_id")
        if requested_leader_id == self.leader_id:
            leader_host, leader_port = self.network.get_peer_address(self.leader_id)
            if leader_host and leader_port:
                send_json_on_sock(conn, {
                    "type": "LEADER_INFO",
                    "sender_id": self.node_id,
                    "leader_id": self.leader_id,
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