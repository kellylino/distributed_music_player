# message_handler.py
import time
# import threading

from common import send_json_on_sock, send_json_to_addr

class MessageHandler:
    def __init__(self, node_id, network_manager, election_module, playback_module):
        self.node_id = node_id
        self.network = network_manager
        self.election = election_module
        self.playback = playback_module

    def handle_message(self, msg, conn):
        m = msg.get("type")

        # Handle election messages separately
        if m == "COORDINATOR":
            self._handle_coordinator_message(msg, conn)
            return
        elif m == "ELECTION":
            def send_response(response_msg):
                send_json_on_sock(conn, response_msg)
            self.election._handle_election_message(msg, send_response)
            return

        handler_map = {
            # Network messages
            "HELLO": self._handle_hello,
            "DISCOVERY_REQUEST": self._handle_discovery_request,
            "DISCOVERY_RESPONSE": self._handle_discovery_response,
            "HEARTBEAT": self._handle_heartbeat,
            "HEARTBEAT_ACK": self._handle_heartbeat_ack,
            "RECONNECT_REQUEST": self._handle_reconnect_request,
            # "RECONNECT_ACK": self._handle_reconnect_ack,

            # Playback messages
            "PLAY_REQUEST": self._handle_play_request,
            "PAUSE_REQUEST": self._handle_pause_request,
            "RESUME_REQUEST": self._handle_resume_request,
            "STOP_REQUEST": self._handle_stop_request,

            # Other messages
            "LEADER_DISCOVERY": self._handle_leader_discovery,
            "CLOCK_SYNC_REQUEST": self._handle_clock_sync_request,
            # "CLOCK_SYNC_RESPONSE": self._handle_clock_sync_response
        }

        if m in handler_map:
            handler_map[m](msg, conn)
        else:
            print(f"[WARNING] Unknown message type: {m}")

    def _handle_hello(self, msg, conn):
        sender_id = msg.get("sender_id")
        sender_host = msg.get("host")
        sender_port = msg.get("port")
        is_leader = msg.get("is_leader")
        leader_id = msg.get("leader_id")

        if sender_host and sender_port:
            self.network.update_peer_info(sender_id, sender_host, sender_port, is_leader)
            print(f"[HELLO] Updated peer info for {sender_id} at {sender_host}:{sender_port}")

        if is_leader:
            print(f"[HELLO_DEBUG] Setting leader_id to {is_leader}")
            self.election.leader_id = is_leader

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
            # print(f"[HELLO] Updated leader to {leader_id} from {sender_id}")

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
        peer_id = msg.get("sender_id")
        peer_host = msg.get("host")
        peer_port = msg.get("port")

        print(f"[RECONNECT] Received reconnect request from {peer_id} at {peer_host}:{peer_port}")

        # Add/update peer info
        if peer_host and peer_port:
            self.network.update_peer_info(peer_id, peer_host, peer_port, False)
            print(f"[RECONNECT] Updated peer info for {peer_id}")

        # Send acknowledgment
        send_json_on_sock(conn, {
            "type": "RECONNECT_ACK",
            "sender_id": self.node_id,
            "status": "accepted"
        })

    def _handle_discovery_response(self, msg, conn):
        for e in msg.get("peers", []):
            pid, h, p = e["peer_id"], e["host"], e["port"]
            is_leader = e.get("is_leader", False)

            if pid == self.node_id:
                continue

            self.network.update_peer_info(pid, h, p, is_leader)

            if is_leader and self.election.leader_id != pid:
                self.election.leader_id = pid

    def _handle_heartbeat(self, msg, conn):
        send_json_on_sock(conn, {
            "type": "HEARTBEAT_ACK",
            "sender_id": self.node_id
        })

    def _handle_heartbeat_ack(self, msg, conn):
        """Handle HEARTBEAT_ACK message - update last_seen for the sender"""
        sender_id = msg.get("sender_id")

        if sender_id:
            # Update the timestamp for the peer that sent the ACK
            self.network.last_seen[sender_id] = time.time()
            # print(f"[HEARTBEAT] Received ACK from {sender_id}")

    def _handle_play_request(self, msg, conn):
        track = msg.get("track")
        # start_time = msg.get("start_time")
        # leader_id = msg.get("leader_id")

        # Calculate delay and schedule playback
        #delay = start_time - time.time()
        self.playback.prepare_and_schedule_play(track)

    def _handle_pause_request(self, msg, conn):
        pause_time = msg.get("pause_time")
        delay = 0.5
        self.playback.prepare_and_schedule_pause(delay)

    def _handle_resume_request(self, msg, conn):
        resume_time = msg.get("resume_time")
        delay = 0.5
        self.playback.prepare_and_schedule_resume(delay)

    def _handle_stop_request(self, msg, conn):
        stop_time = msg.get("stop_time")

        # Calculate delay and schedule stop
        delay = 0.5
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

    # def _handle_coordinator_message(self, msg, conn):
    #     """Handle COORDINATOR message from new leader"""
    #     new_leader_id = msg.get("leader_id")
    #     leader_host = msg.get("host")
    #     leader_port = msg.get("port")

    #     print(f"[ELECTION] Received COORDINATOR from new leader: {new_leader_id}")

    #     self.election.leader_id = new_leader_id
    #     self.election.is_leader = (new_leader_id == self.node_id)

    #     if leader_host and leader_port:
    #         self.network.update_peer_info(new_leader_id, leader_host, leader_port, True)
    #         print(f"[ELECTION] Added new leader {new_leader_id} to peer list at {leader_host}:{leader_port}")

    #         if new_leader_id != self.node_id:
    #             try:
    #                 self.network.connect_to_peer(leader_host, leader_port)
    #                 print(f"[ELECTION] Connected to new leader {new_leader_id}")
    #             except Exception as e:
    #                 print(f"[ELECTION] Failed to connect to new leader: {e}")

    def _handle_coordinator_message(self, msg, conn):
        """Handle COORDINATOR message from new leader"""
        new_leader_id = msg.get("leader_id")
        leader_host = msg.get("host")
        leader_port = msg.get("port")

        print(f"[ELECTION] Received COORDINATOR from new leader: {new_leader_id}")
        print(f"[ELECTION_DEBUG] Validating coordinator: leader_id={new_leader_id}, in_peers={new_leader_id in self.network.peers}")

        # VALIDATION: Only accept coordinators from peers we know about
        # Or if it's a reconnection scenario, we might need to be more flexible
        if new_leader_id not in self.network.peers and new_leader_id != self.node_id:
            print(f"[ELECTION_WARNING] Coordinator {new_leader_id} not in known peers, but accepting for network healing")
            # We still accept it to allow network recovery

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

        # Call the new leader callback
        if hasattr(self.election, 'on_new_leader_callback') and self.election.on_new_leader_callback:
            self.election.on_new_leader_callback(new_leader_id)

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