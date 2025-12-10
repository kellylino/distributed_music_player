# node.py
"""
Main Node Module - Distributed Audio Synchronization System
This module implements the main peer node that coordinates with other nodes
to synchronize audio playback across a distributed network.
"""

import time
import uuid
import random
import textwrap
import argparse
import threading

from playback import AudioPlayer
from election import BullyElection
from network import NetworkManager
from common import send_json_to_addr
from message_handler import MessageHandler

def now():
    """Helper function to get current timestamp in seconds."""
    return time.time()

class PeerNode:
    """
    Represents a peer node in the distributed audio synchronization network.

    Each node can be either a leader or a follower:
    - Leader: Coordinates playback commands and synchronization
    - Follower: Receives and executes commands from the leader

    The node uses the Bully algorithm for leader election and maintains
    connections with other peers through a network manager.
    """

    def __init__(self, host, port, bootstrap=None, is_leader=False):
        self.id = str(uuid.uuid4()) # Unique identifier for this node
        self.host = host
        self.port = port
        self.bootstrap = bootstrap # Existing node to connect to
        self.running = True # Control flag for main loop

        self.lock = threading.RLock()  # Reentrant lock for thread safety

        # Initialize modules
        self.player = AudioPlayer() # Handles audio playback

        # Network manager handles all peer-to-peer communication
        self.network = NetworkManager(
            host, port, self.id,
            on_message_callback=self._handle_message,
            on_peer_update_callback=self._on_peer_update,
            on_leader_update_callback=self._update_leader_info
        )

        # Election module handles leader election using Bully algorithm
        self.election = BullyElection(
            node_id=self.id,
            peers=self.network.peers,
            get_peer_address=self.network.get_peer_address,
            is_leader=is_leader,
            leader_id=self.id if is_leader else None
        )

        # Message handler processes incoming messages
        self.message_handler = MessageHandler(
            self.id, self.network, self.election, self.player
        )

    def _handle_message(self, msg, conn):
        """Callback for handling incoming messages from network."""
        self.message_handler.handle_message(msg, conn)

    def _update_leader_info(self, leader_id):
        """Update leader information in election module when network detects change."""
        self.election.leader_id = leader_id

    def _on_peer_update(self, action, peer_id):
        """
        Callback when peer status changes (added/removed).

        If the leader disconnects, start a new election with random delay
        to avoid simultaneous elections.
        """

        current_leader_id = self.leader_id

        if action == "removed" and peer_id == current_leader_id:
            delay = random.uniform(0.3, 1.0) # Random delay to prevent collisions
            threading.Timer(delay, self.elect_new_leader).start()
        elif action == "removed":
            # print(f"[Peer] {peer_id} removed, but it's not the leader (leader is {current_leader_id})")
            pass

    def _on_new_leader(self, leader_id):
        """
        Callback when a new leader is elected.

        Updates local state and if this node becomes leader,
        announces leadership to all peers.
        """

        self.leader_id = leader_id
        self.is_leader = (leader_id == self.id)

        if self.is_leader:
            print("[LEADER] I am the new leader!")
            # Announce leadership to all peers
            threading.Thread(target=self._announce_new_leadership, daemon=True).start()
            # Request all peers to reconnect for state synchronization
            threading.Thread(target=self.message_handler.broadcast_reconnect_request, daemon=True).start()
        else:
            print(f"[FOLLOWER] I am a follower, new leader: {self.leader_id}")

    def elect_new_leader(self):
        """
        Delegate election to the election module

        Start a new leader election process.
        """

        self.election.start_election(on_new_leader_callback=self._on_new_leader)

    def _announce_new_leadership(self):
        """
        Announce this node as the new leader to all known peers.

        Uses exponential backoff retry for failed connections.
        """

        def send_to_peer(pid, message):
            """Helper to send announcement to a specific peer with retry."""
            host, port = self.network.get_peer_address(pid)
            if host and port:
                message["host"] = self.host
                message["port"] = self.port
                try:
                    send_json_to_addr(host, port, message)
                except Exception as e:
                    print(f"[ANNOUNCE] Failed to send to {pid}: {e}")
                    # Retry with connection attempt
                    try:
                        self.network.connect_to_peer(host, port)
                        send_json_to_addr(host, port, message)
                        print(f"[ANNOUNCE] Successfully resent to {pid}")
                    except Exception as e2:
                        print(f"[ANNOUNCE] Final failure for {pid}: {e2}")
            else:
                print(f"[ANNOUNCE] No address for peer {pid}")

        # print(f"[ANNOUNCE] Announcing leadership to {len(self.network.get_peers())} peers")
        self.election.announce_leadership(send_to_peer_callback=send_to_peer)

    # === Leader Broadcast commands ===
    # These methods are called by the leader to synchronize playback across the network

    def broadcast_play(self, track, leader_id):
        """
        Broadcast play command to all peers.

        Args:
            track: Audio track filename to play
            leader_id: ID of the leader node (for verification)
        """

        self.network.broadcast_message("PLAY_REQUEST", {
            "track": track,
            "index": self.player.current_index,
        }, leader_id)

    def broadcast_pause(self, leader_id):
        """
        Broadcast pause command to all peers.

        Includes future delay compensation to account for network latency.
        """

        future_delay = 0.3  # Compensation for network latency
        pause_time = now()
        current_position = self.player.get_current_position() + future_delay

        self.network.broadcast_message("PAUSE_REQUEST", {
            "pause_time": pause_time,
            "pause_position": current_position
        }, leader_id)

    def broadcast_resume(self, leader_id):
        """Broadcast resume command to all peers."""

        if not self.player.current_track:
            print("[RESUME] No track to resume - play a track first")
            return

        resume_time = now()
        track = self.player.current_track
        self.network.broadcast_message("RESUME_REQUEST", {
            "track": track,
            "resume_time": resume_time
        }, leader_id)

    def broadcast_stop(self, leader_id):
        """Broadcast stop command to all peers"""

        stop_time = now()
        self.network.broadcast_message("STOP_REQUEST", {
            "stop_time": stop_time
        }, leader_id)

    # --- CLI Helper Methods ---
    def show_peers(self):
        """Display connected peers with their status information."""

        peers = self.network.get_peers()
        last_seen = self.network.last_seen
        # clock_offsets = self.network.clock_offsets

        lines = []
        lines.append("CONNECTED PEERS")
        lines.append(f"Total peers: {len(peers)}")
        lines.append("")  # blank line

        for pid, (h, p) in peers.items():
            age = now() - last_seen.get(pid, 0) if last_seen.get(pid) else float("inf")
            # off = clock_offsets.get(pid, None)
            leader_flag = " (LEADER)" if pid == self.leader_id else ""

            lines.append(f"{pid}{leader_flag}")
            lines.append(f"  Address       : {h}:{p}")
            lines.append(f"  Last seen     : {age:.2f}s")
            lines.append("")  # space between peers

        # Box width
        max_len = max(len(line) for line in lines)
        border = "+" + "-" * (max_len + 2) + "+"

        # Print box
        print()
        print(border)
        for line in lines:
            print(f"| {line.ljust(max_len)} |")
        print(border)

    def debug_status(self):
        """Display debug information about node status."""

        peers = self.network.get_peers()
        peer_ids = list(peers.keys())

        lines = [
            "DEBUG STATUS",
            f"My ID          : {self.id}",
            f"I am leader    : {self.is_leader}",
            f"Current leader : {self.leader_id}",
            "Known peers    :"
        ]

        # Add each peer as a separate line, indented
        for pid in peer_ids:
            lines.append(f"  - {pid}")

        lines.append(f"Bootstrap      : {self.bootstrap}")

        max_len = max(len(line) for line in lines)
        border = "+" + "-" * (max_len + 2) + "+"

        # Print box
        print()
        print(border)
        for line in lines:
            print(f"| {line.ljust(max_len)} |")
        print(border)

    def print_commands_box():
        """
        Display a neatly formatted commands menu inside a box.
        """
        commands = {
            "Network": ": peers, debug",
            "Playback": ": play <index>, pause, resume, stop, next, prev",
            "Info": ": list, status",
            "Control": ": exit"
        }

        # Build lines with wrapping for long descriptions
        lines = ["COMMANDS"]
        wrapper = textwrap.TextWrapper(width=60, subsequent_indent=" " * 14)

        for key, desc in commands.items():
            wrapped = wrapper.wrap(desc)
            lines.append(f"  {key:<10}{wrapped[0]}")
            for w in wrapped[1:]:
                lines.append(f"  {'':<10}{w}")

        max_len = max(len(line) for line in lines)
        border = "+" + "-" * (max_len + 2) + "+"

        # Print the box
        print()
        print(border)
        for line in lines:
            print(f"| {line.ljust(max_len)} |")
        print(border)

    # --- Playback Control Methods (Leader Only) ---

    def play_next(self):
        """Play next track in playlist with synchronized start."""

        # Stop current playback first
        self.stop_immediate()

        # Advance local pointer and broadcast play command
        track = self.player.next_track()
        self.broadcast_play(track, leader_id=self.id)

    def play_previous(self):
        """Play previous track in playlist with synchronized start."""

        # Stop current playback first
        self.stop_immediate()

        # Get previous track
        track = self.player.previous_track()
        self.broadcast_play(track, leader_id=self.id)

    def play_index(self, index):
        """Play specific track by index with synchronized start."""

        self.stop_immediate()

        track = self.player.play_index(index)
        self.broadcast_play(track, leader_id=self.id)

    def pause_immediate(self):
        """Pause playback and broadcast to all peers."""

        if not self.player.get_playback_state()['is_playing']:
            # print("[PAUSE] not currently playing")
            return

        self.broadcast_pause(leader_id=self.id)

    def resume_immediate(self):
        """Resume playback and broadcast to all peers."""

        if self.player.get_playback_state()['is_playing']:
            # print("[RESUME] already playing")
            return

        self.broadcast_resume(leader_id=self.id)

    def stop_immediate(self):
        """Stop playback immediately and broadcast to all peers."""

        if not self.player.get_playback_state()['is_playing']:
            # print("[STOP] not currently playing")
            return

        self.broadcast_stop(leader_id=self.id)

    def list_tracks(self):
        """List available tracks"""
        return self.player.get_playlist()

    # Property accessors for compatibility with existing code

    @property
    def is_leader(self):
        """Get whether this node is the leader."""
        return self.election.is_leader

    @is_leader.setter
    def is_leader(self, value):
        """Set leader status."""
        self.election.is_leader = value

    @property
    def leader_id(self):
        """Get current leader ID."""
        return self.election.leader_id

    @leader_id.setter
    def leader_id(self, value):
        """Set current leader ID."""
        self.election.leader_id = value

    # --- Server Start Method ---

    def start(self):
        """Start the node server and all background threads."""
        self.network.start()

# --- CLI Entry Point ---

def main():
    """
    Command-line interface for running a peer node.

    Usage:
        python node.py <host> <port> [--bootstrap HOST PORT] [--leader]

    Examples:
        # Start as leader on localhost:5000
        python node.py localhost 5000 --leader

        # Start as follower and connect to existing node
        python node.py localhost 5001 --bootstrap localhost 5000
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("--bootstrap", nargs=2, metavar=("HOST","PORT"))
    parser.add_argument("--leader", action="store_true")
    args = parser.parse_args()

    bootstrap = tuple(args.bootstrap) if args.bootstrap else None
    node = PeerNode(args.host, args.port, bootstrap=bootstrap, is_leader=args.leader)
    node.start()

    # Connect to bootstrap node if provided
    if bootstrap:
        threading.Thread(
            target=node.network.connect_to_peer,
            args=(bootstrap[0], int(bootstrap[1])),
            daemon=True
        ).start()

    # Interactive command loop
    PeerNode.print_commands_box()

    try:
        while True:
            cmd = input("> ").strip().split()
            if not cmd: continue

            if cmd[0] == "peers":
                node.show_peers()

            elif cmd[0] == "play":
                if not node.is_leader:
                    print("only leader can schedule")
                else:
                    if len(cmd) > 1:
                        try:
                            index = int(cmd[1])
                            node.play_index(index)
                        except ValueError:
                            print("Invalid index. Usage: play <index>")
                    else:
                        print("Usage: play <index>")

            elif cmd[0] == "list":
                tracks = node.list_tracks()
                if tracks:
                    lines = ["AVAILABLE TRACKS", ""]
                    for i, track in enumerate(tracks):
                        current_flag = " [CURRENT]" if i == node.player.current_index else ""
                        lines.append(f"  {i:2d}: {track}{current_flag}")

                    max_len = max(len(line) for line in lines)
                    border = "+" + "-" * (max_len + 2) + "+"

                    print()
                    print(border)
                    for line in lines:
                        print(f"| {line.ljust(max_len)} |")
                    print(border)
                    print(f"Total: {len(tracks)} tracks")
                else:
                    print("No tracks found in music directory")

            elif cmd[0] == "pause":
                if not node.is_leader:
                    print("only leader can schedule")
                else:
                    node.pause_immediate()

            elif cmd[0] == "resume":
                if not node.is_leader:
                    print("only leader can schedule")
                else:
                    node.resume_immediate()

            elif cmd[0] == "debug":
                node.debug_status()

            elif cmd[0] == "status":
                playback_state = node.player.get_playback_state()
                current_track = playback_state['current_track'] or "None"
                is_playing = playback_state['is_playing']
                current_index = playback_state['current_index']
                pause_position = f"{playback_state['pause_position']:.2f}s"

                if is_playing:
                    current_pos = f"{node.player.get_current_position():.2f}s"
                else:
                    current_pos = "N/A"

                lines = [
                    "PLAYBACK STATUS",
                    f"Current track   : {current_track}",
                    f"Playing         : {is_playing}",
                    f"Current index   : {current_index}",
                    f"Pause position  : {pause_position}",
                    f"Current position: {current_pos}",
                ]

                max_len = max(len(line) for line in lines)
                border = "+" + "-" * (max_len + 2) + "+"

                print()
                print(border)
                for line in lines:
                    print(f"| {line.ljust(max_len)} |")
                print(border)

            elif cmd[0] == "stop":
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.stop_immediate()
                    print("Playback stopped")

            elif cmd[0] == "next":
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.play_next()

            elif cmd[0] == "prev":
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.play_previous()

            elif cmd[0] == "exit":
                # Stop playback before exiting if no other peers
                peers = node.network.get_peers()

                if not peers:
                    node.stop_immediate()
                break

            else:
                print("\nUnknown command")
                PeerNode.print_commands_box()

    except KeyboardInterrupt:
        pass
    finally:
        node.running = False
        time.sleep(0.5)
        print("--------")
        print("Goodbye!")

if __name__ == "__main__":
    main()