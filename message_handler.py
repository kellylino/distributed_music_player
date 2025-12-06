# message_handler.py
"""
Message Handler Module - Processes All Incoming Network Messages
This module acts as a dispatcher for different message types,
routing each message to the appropriate handler function.
"""

import time
import threading

from common import send_json_on_sock, send_json_to_addr

def now():
    """Helper function to get current timestamp."""
    return time.time()

class MessageHandler:
    """
    Central message processor for the node.

    Routes incoming messages to appropriate handlers based on message type.
    Handles network, election, playback, and synchronization messages.
    """

    def __init__(self, node_id, network_manager, election_module, playback_module):
        """
        Initialize message handler.

        Args:
            node_id (str): This node's ID
            network_manager (NetworkManager): Network communication module
            election_module (BullyElection): Leader election module
            playback_module (AudioPlayer): Audio playback module
        """

        self.node_id = node_id
        self.network = network_manager
        self.election = election_module
        self.playback = playback_module

    def handle_message(self, msg, conn):
        """
        Main message routing function.

        Args:
            msg (dict): The message to handle
            conn (socket): Connection the message arrived on
        """

        m = msg.get("type")

        # Special handling for election messages
        if m == "COORDINATOR":
            self._handle_coordinator_message(msg, conn)
            return
        elif m == "ELECTION":
            # Wrap connection in response function for election module
            def send_response(response_msg):
                send_json_on_sock(conn, response_msg)
            self.election._handle_election_message(msg, send_response)
            return

        # Dispatch table for all other message types
        handler_map = {
            # Network messages
            "HELLO": self._handle_hello,
            "DISCOVERY_REQUEST": self._handle_discovery_request,
            "DISCOVERY_RESPONSE": self._handle_discovery_response,
            "HEARTBEAT": self._handle_heartbeat,
            "HEARTBEAT_ACK": self._handle_heartbeat_ack,
            "RECONNECT_REQUEST": self._handle_reconnect_request,

            # Playback control (from leader)
            "PLAY_REQUEST": self._handle_play_request,
            "PAUSE_REQUEST": self._handle_pause_request,
            "RESUME_REQUEST": self._handle_resume_request,
            "STOP_REQUEST": self._handle_stop_request,

            # State synchronization
            "STATE_SYNC_REQUEST": self._handle_state_sync_request,
            "STATE_SYNC_RESPONSE": self._handle_state_sync_response,
        }

        if m in handler_map:
            handler_map[m](msg, conn)
        else:
            print(f"[WARNING] Unknown message type: {m}")

    def _handle_hello(self, msg, conn):
        """
        Handle HELLO message from new peer.

        HELLO messages are exchanged when nodes first connect.
        Includes information about leadership status.
        """

        sender_id = msg.get("sender_id")
        sender_host = msg.get("host")
        sender_port = msg.get("port")
        is_leader = msg.get("is_leader")
        leader_id = msg.get("leader_id")

        # Update peer information
        if sender_host and sender_port:
            self.network.update_peer_info(sender_id, sender_host, sender_port, is_leader)
            print(f"[HELLO] Updated peer info for {sender_id} at {sender_host}:{sender_port}")

        # Update leader information if sender is leader
        if is_leader:
            self.election.leader_id = leader_id

        # Send response with own information
        send_json_on_sock(conn, {
            "type": "HELLO",
            "sender_id": self.node_id,
            "host": self.network.host,
            "port": self.network.port,
            "is_leader": self.election.is_leader,
            "leader_id": self.election.leader_id
        })

    def _handle_discovery_request(self, msg, conn):
        """
        Handle request for network topology information.

        Returns list of all known peers including their leadership status.
        Used for network bootstrapping and recovery.
        """

        peers = self.network.get_peers()

        # Build peer list starting with ourselves
        plist = [{"peer_id": self.node_id, "host": self.network.host, "port": self.network.port, "is_leader": self.election.is_leader}]

        # Add all other known peers
        for pid, (h, p) in peers.items():
            plist.append({"peer_id": pid, "host": h, "port": p, "is_leader": (pid == self.election.leader_id)})

        send_json_on_sock(conn, {
            "type": "DISCOVERY_RESPONSE",
            "sender_id": self.node_id,
            "peers": plist
        })

    def _handle_reconnect_request(self, msg, conn):
        """
        Handle reconnection request from peer (usually new leader).

        Used when leader changes and new leader needs to establish connections with all followers.
        """

        peer_id = msg.get("sender_id")
        peer_host = msg.get("host")
        peer_port = msg.get("port")

        # Add/update peer info
        if peer_host and peer_port:
            self.network.update_peer_info(peer_id, peer_host, peer_port, False)

        # Send acknowledgment
        send_json_on_sock(conn, {
            "type": "RECONNECT_ACK",
            "sender_id": self.node_id,
            "status": "accepted"
        })

    def _handle_discovery_response(self, msg, conn):
        """
        Handle response to discovery request.

        Updates peer list and leader information from the response.
        If leader is discovered, request state synchronization.
        """

        leader_found = False

        for e in msg.get("peers", []):
            pid, h, p = e["peer_id"], e["host"], e["port"]
            is_leader = e.get("is_leader", False)

            if pid == self.node_id:
                continue

            self.network.update_peer_info(pid, h, p, is_leader)

            if is_leader and self.election.leader_id != pid:
                self.election.leader_id = pid
                leader_found = True

        # If a leader is found and the node is not the leader, sync state
        if leader_found and not self.election.is_leader:
            threading.Thread(target=self._request_state_sync, daemon=True).start()


    def _handle_heartbeat(self, msg, conn):
        """Handle heartbeat message - respond with acknowledgment."""

        send_json_on_sock(conn, {
            "type": "HEARTBEAT_ACK",
            "sender_id": self.node_id
        })

    def _handle_heartbeat_ack(self, msg, conn):
        """Handle HEARTBEAT_ACK message - Updates last_seen timestamp for the sender to mark it as alive."""

        sender_id = msg.get("sender_id")
        if sender_id:
            self.network.last_seen[sender_id] = now()

    def _handle_play_request(self, msg, conn):
        """
        Handle play command from leader.

        Schedules playback of specified track with appropriate delay to synchronize with other nodes.
        """

        track = msg.get("track")
        index = msg.get("index")
        self.playback.current_index = index
        delay = 0.3 # Fixed delay for synchronization
        self.playback.prepare_and_schedule_play(track, delay)

    def _handle_pause_request(self, msg, conn):
        """
        Handle pause command from leader.

        Calculates appropriate delay based on message timestamp to synchronize pause across all nodes.
        """

        pause_time = msg.get("pause_time")
        pause_position = msg.get("pause_position")
        self.playback.pause_position = pause_position
        current_time = now()
        delay = current_time - pause_time
        # print(f"[Pause delay] {delay}")

        delay = max(delay, 0.3) # Minimum delay for reliability
        self.playback.prepare_and_schedule_pause(delay)

    def _handle_resume_request(self, msg, conn):
        """Handle resume command from leader."""
        track = msg.get("track")
        delay = 0.3
        self.playback.prepare_and_schedule_resume(track, delay)

    def _handle_stop_request(self, msg, conn):
        """Handle stop command from leader."""
        delay = 0.3
        self.playback.prepare_and_schedule_stop(delay)

    def _handle_coordinator_message(self, msg, conn):
        """
        Handle COORDINATOR message from new leader

        Validates and processes leader announcement.
        """
        new_leader_id = msg.get("leader_id")
        leader_host = msg.get("host")
        leader_port = msg.get("port")

        # Update leadership state
        self.election.leader_id = new_leader_id
        self.election.is_leader = (new_leader_id == self.node_id)

        # Add leader to peer list
        if leader_host and leader_port:
            self.network.update_peer_info(new_leader_id, leader_host, leader_port, True)

        # Connect to new leader
        if new_leader_id != self.node_id:
            try:
                self.network.connect_to_peer(leader_host, leader_port)
            except Exception as e:
                print(f"[ELECTION] Failed to connect to new leader: {e}")

        # Call the new leader callback
        if hasattr(self.election, 'on_new_leader_callback') and self.election.on_new_leader_callback:
            self.election.on_new_leader_callback(new_leader_id)

    def broadcast_reconnect_request(self):
        """Broadcast reconnect request to all known peers to ensure all peers know the new leader's address"""
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
            except Exception as e:
                print(f"[RECONNECT] Failed to send to {pid}: {e}")

    def _request_state_sync(self):
        """Request current playback state from leader"""

        # Only followers should request state sync
        if self.election.is_leader or not self.election.leader_id:
            print(f"[STATE_SYNC] Cannot request state sync: is_leader={self.election.is_leader}, leader_id={self.election.leader_id}")
            return

        leader_host, leader_port = self.network.get_peer_address(self.election.leader_id)

        if leader_host and leader_port:
            try:
                send_json_to_addr(leader_host, leader_port, {
                    "type": "STATE_SYNC_REQUEST",
                    "sender_id": self.node_id,
                    "request_time": now()
                })
            except Exception as e:
                print(f"[STATE_SYNC] Failed to request state from leader: {e}")
        else:
            print(f"[STATE_SYNC] Cannot find leader address for {self.election.leader_id}")

    def _handle_state_sync_request(self, msg, conn):
        """
        Handle state synchronization request from follower.

        Leader sends current playback state to requesting node.
        Includes track, playback status, and position information.
        """

        # Ignoring state sync request - if it is not the leader
        if not self.election.is_leader:
            return

        sender_id = msg.get("sender_id")
        request_time = msg.get("request_time")

        # Get current playback state
        playback_state = self.playback.get_playback_state()
        current_position = self.playback.get_current_position()

        # Send response directly to the sender instead of using the incoming connection
        sender_host, sender_port = self.network.get_peer_address(sender_id)
        if sender_host and sender_port:
            try:
                send_json_to_addr(sender_host, sender_port, {
                    "type": "STATE_SYNC_RESPONSE",
                    "sender_id": self.node_id,
                    "playback_state": playback_state,
                    "current_track": playback_state['current_track'],
                    "is_playing": playback_state['is_playing'],
                    "current_position": current_position,
                    "current_index": playback_state['current_index'],
                    "sync_time": request_time,
                })
            except Exception as e:
                print(f"[STATE_SYNC] Failed to send direct response to {sender_id}: {e}")
        else:
            print(f"[STATE_SYNC] Cannot find address for {sender_id}")

    def _handle_state_sync_response(self, msg, conn):
        """
        Handle state synchronization response from leader.

        Updates local playback state to match leader's state.
        Handles both playing and paused states with position adjustment.
        """

        if self.election.is_leader:
            return

        current_track = msg.get("current_track")
        is_playing = msg.get("is_playing")
        current_position = msg.get("current_position")
        current_index = msg.get("current_index")
        sync_time = msg.get("sync_time")

        # Update local playback state to match the cluster
        if current_track:
            # Check if track exists locally
            playlist = self.playback.get_playlist()
            if current_track not in playlist:
                # Try to find track by index
                if 0 <= current_index < len(playlist):
                    current_track = playlist[current_index]
                else:
                    return

            # Set the current track and index
            with self.playback.lock:
                self.playback.current_track = current_track
                self.playback.current_index = current_index

            if is_playing:
                # Calculate adjusted start time based on sync time
                current_time = now()
                time_since_sync = current_time - sync_time

                # Account for network latency (simple estimation)
                adjusted_position = current_position + time_since_sync/2

                print(f"[STATE_SYNC] Starting playback from adjusted position: {adjusted_position:.2f}s")

                # Load and start playing from the calculated position
                if hasattr(self.playback, '_start_play_local_from_position'):
                    self.playback._start_play_local_from_position(current_track, adjusted_position)
                else:
                    # Fallback: start from beginning
                    print(f"[STATE_SYNC] Fallback: starting from beginning")
                    self.playback._start_play_local(current_track)
            else:
                # Set paused state
                with self.playback.lock:
                    self.playback.is_playing = False
                    self.playback.current_track = current_track
                    self.playback.current_index = current_index
                    self.playback.pause_position = current_position
        else:
            print(f"[STATE_SYNC] No current track to sync")