# node.py
import os
import time
import uuid
import random
import argparse
import threading

from playback import AudioPlayer
from election import BullyElection
from network import NetworkManager
from common import send_json_to_addr
from message_handler import MessageHandler

MUSIC_DIR = "music"

HEARTBEAT_INTERVAL = 2.0
MONITOR_INTERVAL = 1.0
SYNC_INTERVAL = 4.0
PEER_TIMEOUT = 4.0

def now():
    return time.time()

class PeerNode:
    def __init__(self, host, port, bootstrap=None, is_leader=False):
        self.id = str(uuid.uuid4())
        self.host = host
        self.port = port
        self.bootstrap = bootstrap
        self.running = True

        self.lock = threading.RLock()

        # Initialize modules
        self.player = AudioPlayer()
        self.network = NetworkManager(
            host, port, self.id,
            on_message_callback=self._handle_message,
            on_peer_update_callback=self._on_peer_update
        )
        self.election = BullyElection(
            node_id=self.id,
            peers=self.network.peers,  # Reference to network's peers
            get_peer_address=self.network.get_peer_address,
            is_leader=is_leader,
            leader_id=self.id if is_leader else None
        )
        self.message_handler = MessageHandler(
            self.id, self.network, self.election, self.player
        )

        # Set up election callbacks
        self.election.on_new_leader = self._on_new_leader

        print(f"[INIT] id={self.id} host={host}:{port} leader={self.is_leader}")

    def _handle_message(self, msg, conn):
        self.message_handler.handle_message(msg, conn)

    def _on_peer_update(self, action, peer_id):
        """Handle peer updates from network manager"""
        if action == "removed" and peer_id == self.leader_id:
            print(f"[ELECTION] Leader {peer_id} disconnected — starting election")
            delay = random.uniform(0.5, 2.0)
            threading.Timer(delay, self.elect_new_leader).start()

    def _on_new_leader(self, leader_id):
        """Callback when a new leader is elected"""
        print(f"[CALLBACK] New leader elected: {leader_id}")
        self.leader_id = leader_id
        self.is_leader = (leader_id == self.id)

        if self.is_leader:
            print("[CALLBACK] I am the new leader!")
            # New leader responsibilities
            threading.Thread(target=self._announce_new_leadership, daemon=True).start()
            threading.Thread(target=self.sync_all_peers, daemon=True).start()
            # Reconnect to all peers
            threading.Thread(target=self._reconnect_as_leader, daemon=True).start()
            # Broadcast reconnect request
            threading.Thread(target=self.message_handler.broadcast_reconnect_request, daemon=True).start()
        else:
            print(f"[CALLBACK] I am a follower, new leader: {leader_id}")
            # Ensure connection to new leader
            self._ensure_connected_to_leader()

    def _get_peer_address(self, peer_id):
        """Helper method for election module to get peer addresses"""
        with self.lock:
            peers = self.network.get_peers()
            return peers.get(peer_id, (None, None))

    def elect_new_leader(self):
        """Delegate election to the election module"""
        self.election.start_election(on_new_leader_callback=self._on_new_leader)

    def _announce_new_leadership(self):
        """Announce leadership using election module"""
        def send_to_peer(pid, message):
            host, port = self.network.get_peer_address(pid)
            if host and port:
                message["host"] = self.host  # Include connection info
                message["port"] = self.port
                try:
                    send_json_to_addr(host, port, message)
                    print(f"[ANNOUNCE] Sent COORDINATOR to {pid}")
                except Exception as e:
                    print(f"[ANNOUNCE] Failed to send to {pid}: {e}")
                    # Try to reconnect and send again
                    try:
                        self.network.connect_to_peer(host, port)
                        send_json_to_addr(host, port, message)
                        print(f"[ANNOUNCE] Successfully resent to {pid}")
                    except Exception as e2:
                        print(f"[ANNOUNCE] Final failure for {pid}: {e2}")
            else:
                print(f"[ANNOUNCE] No address for peer {pid}")

        print(f"[ANNOUNCE] Announcing leadership to {len(self.network.get_peers())} peers")
        self.election.announce_leadership(send_to_peer_callback=send_to_peer)

    def _ensure_connected_to_leader(self):
        """Ensure we're connected to the current leader"""
        if not self.leader_id or self.leader_id == self.id:
            return

        leader_host, leader_port = self.network.get_peer_address(self.leader_id)
        if leader_host and leader_port:
            print(f"[CONNECT] Already connected to leader {self.leader_id}")
            return

        # If leader not in peers, try to discover through other peers
        print(f"[CONNECT] Leader {self.leader_id} not in direct peers, discovering...")
        self._discover_leader()

    def _reconnect_to_all_peers(self):
        """Reconnect to all known peers to rebuild network topology"""
        print("[RECONNECT] Reconnecting to all known peers as new leader...")

        # Get all peer addresses from the network manager
        peers = self.network.get_peers()

        for pid, (host, port) in peers.items():
            if pid == self.id:
                continue

            print(f"[RECONNECT] Attempting to reconnect to {pid} at {host}:{port}")
            try:
                # Try to establish direct connection
                self.network.connect_to_peer(host, port)
                print(f"[RECONNECT] Successfully reconnected to {pid}")
            except Exception as e:
                print(f"[RECONNECT] Failed to reconnect to {pid}: {e}")

    def _reconnect_as_leader(self):
        """Reconnect to all known peers when becoming leader"""
        print("[LEADER] Reconnecting to all known peers...")
        peers = self.network.get_peers()

        for pid, (host, port) in peers.items():
            if pid == self.id:
                continue
            try:
                print(f"[LEADER] Reconnecting to {pid} at {host}:{port}")
                self.network.connect_to_peer(host, port)
            except Exception as e:
                print(f"[LEADER] Failed to reconnect to {pid}: {e}")

    def _discover_leader(self):
        """Discover leader through other peers"""
        peers = self.network.get_peers()
        for pid, (host, port) in peers.items():
            if pid == self.id:
                continue

            try:
                print(f"[DISCOVER] Asking {pid} about leader {self.leader_id}")
                resp = send_json_to_addr(host, port, {
                    "type": "LEADER_DISCOVERY",
                    "sender_id": self.id,
                    "leader_id": self.leader_id
                })
                if resp and resp.get("type") == "LEADER_INFO":
                    leader_host = resp.get("host")
                    leader_port = resp.get("port")
                    if leader_host and leader_port:
                        print(f"[DISCOVER] Got leader info: {leader_host}:{leader_port}")
                        self.network.connect_to_peer(leader_host, leader_port)
                        return
            except Exception as e:
                print(f"[DISCOVER] Failed to get leader info from {pid}: {e}")

    def connect_to_peer(self, host, port):
            """Connect to a peer (for external use)"""
            self.network.connect_to_peer(host, port)

    def _scan_music(self):
        if not os.path.isdir(MUSIC_DIR):
            return []
        files = [f for f in os.listdir(MUSIC_DIR) if f.lower().endswith((".mp3", ".wav", ".ogg"))]
        files.sort()
        return files

    # --- server ---
    def start(self):
        self.network.start()

        if self.bootstrap:
            threading.Thread(
                target=self.network.connect_to_peer,
                args=(self.bootstrap[0], int(self.bootstrap[1])),
                daemon=True
            ).start()

   # --- playback ---
    """Delegate to AudioPlayer"""
    def _prepare_and_schedule_play(self, track, delay):
        self.player.prepare_and_schedule_play(track, delay)

    def _prepare_and_schedule_pause(self, delay):
        self.player.prepare_and_schedule_pause(delay)

    def _prepare_and_schedule_resume(self, delay):
        self.player.prepare_and_schedule_resume(delay)

    def _start_play_local(self, track):
        return self.player.play_immediate(track)

    def _pause_local(self):
        self.player.pause_immediate()

    def _resume_local(self):
        self.player.resume_immediate()

    def _stop_local(self):
        self.player.stop_immediate()

    # === Leader Broadcast Commands ===
    def broadcast_play(self, track, start_time, leader_id=None):
        if leader_id is None:
            leader_id = self.id

        self.network.broadcast_message("PLAY_REQUEST", {
            "track": track,
            "start_time": start_time
        }, leader_id)

        # leader also plays locally at start_time
        local_delay = start_time - now()
        print(f"[LEADER] scheduled {track} at leader_time={start_time:.3f} (in {local_delay:.3f}s)")
        self._prepare_and_schedule_play(track, local_delay)

    def broadcast_pause(self, pause_time, leader_id=None):
        if leader_id is None:
            leader_id = self.id

        print(f"[DEBUG] Broadcasting pause to peers")
        self.network.broadcast_message("PAUSE_REQUEST", {
            "pause_time": pause_time
        }, leader_id)

        # leader also pauses locally
        local_delay = pause_time - now()
        print(f"[LEADER] scheduled pause at leader_time={pause_time:.3f} (in {local_delay:.3f}s)")
        self._prepare_and_schedule_pause(local_delay)

    def broadcast_resume(self, resume_time, leader_id=None):
        if leader_id is None:
            leader_id = self.id

        self.network.broadcast_message("RESUME_REQUEST", {
            "resume_time": resume_time
        }, leader_id)

        # leader also resumes locally
        local_delay = resume_time - now()
        print(f"[LEADER] scheduled resume at leader_time={resume_time:.3f} (in {local_delay:.3f}s)")
        self._prepare_and_schedule_resume(local_delay)

    def broadcast_stop(self, stop_time=None, leader_id=None):
        """Broadcast stop command to all peers"""
        if leader_id is None:
            leader_id = self.id
        if stop_time is None:
            stop_time = now()

        print(f"[DEBUG] Broadcasting stop to peers")
        self.network.broadcast_message("STOP_REQUEST", {
            "stop_time": stop_time
        }, leader_id)

        # leader also stops locally
        self._stop_local()

    # --- clock sync (optional) ---
    def sync_clock_with_peer(self, pid, host, port):
        """
        Simple direct-sync: leader connects, sends request, receives follower_time and computes offset.
        offset = follower_time - (t0 + rtt/2).
        Stores offset in self.clock_offsets[pid]
        """
        try:
            t0 = now()
            resp = send_json_to_addr(host, port, {"type":"CLOCK_SYNC_REQUEST", "sender_id": self.id, "leader_time": t0})
            t2 = now()
            if resp and resp.get("type") == "CLOCK_SYNC_RESPONSE":
                follower_time = resp.get("follower_time")
                rtt = t2 - t0
                midpoint = t0 + rtt/2.0
                offset = follower_time - midpoint
                with self.lock:
                    self.network.clock_offsets[pid] = offset
                    self.network.last_seen[pid] = now()
                print(f"[CLOCK] {pid} rtt={rtt:.4f}s offset={offset:.4f}s")
        except Exception:
            pass

    def sync_all_peers(self):
        peers = self.network.get_peers()
        threads = []
        for pid,(h,p) in peers.items():
            if pid == self.id: continue
            t = threading.Thread(target=self.sync_clock_with_peer, args=(pid,h,p), daemon=True)
            t.start(); threads.append(t)
        for t in threads:
            t.join(timeout=2.0)
        print("[CLOCK] sync round finished")

    # --- heartbeat + monitor ---
    def _heartbeat_loop(self):
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                peers_copy = dict(self.peers)
            for pid,(h,p) in peers_copy.items():
                try:
                    resp = send_json_to_addr(h,p, {"type":"HEARTBEAT", "sender_id": self.id})
                    if resp and resp.get("type") == "HEARTBEAT_ACK":
                        with self.lock:
                            self.last_seen[pid] = now()
                except:
                    pass

    def _monitor_loop(self):
        while self.running:
            time.sleep(MONITOR_INTERVAL)
            nowt = now()
            removed = []
            leader_lost = False

            with self.lock:
                # Create a copy to avoid modification during iteration
                peers_copy = dict(self.network.get_peers())
                last_seen_copy = dict(self.network.last_seen)

                for pid, last in last_seen_copy.items():
                    if pid == self.id:
                        continue
                    if nowt - last > PEER_TIMEOUT:
                        print(f"[TIMEOUT] Peer {pid} timed out, but keeping for reconnection")
                        # Don't remove from peers, just mark as needing reconnection
                        removed.append(pid)

                        # Check if the timed out peer was the leader
                        if pid == self.leader_id:
                            leader_lost = True

            # Process leader loss
            if leader_lost:
                print("[ELECTION] Leader disappeared — starting Bully election")
                # Start election after a random delay to avoid conflicts
                delay = random.uniform(0.5, 2.0)
                threading.Timer(delay, self.elect_new_leader).start()

    def _reconnect_to_peers_after_leader_loss(self):
        """When leader is lost, try to reconnect to other known peers"""
        print("[RECONNECT] Attempting to reconnect to other peers...")

        # Get all known peer addresses except self and the lost leader
        peers_to_try = []
        peers = self.network.get_peers()  # Use network manager
        for pid, (host, port) in peers.items():
            if pid != self.id and pid != self.leader_id:
                peers_to_try.append((host, port))

        # Also try to rediscover from remaining peers
        for host, port in peers_to_try:
            try:
                print(f"[RECONNECT] Trying to reconnect to {host}:{port}")
                # Send discovery request to rebuild peer list
                resp = send_json_to_addr(host, port, {
                    "type": "DISCOVERY_REQUEST",
                    "sender_id": self.id
                })
                if resp and resp.get("type") == "DISCOVERY_RESPONSE":
                    print(f"[RECONNECT] Successfully rediscovered peers from {host}:{port}")
                    # The discovery response will trigger new connections automatically
            except Exception as e:
                print(f"[RECONNECT] Failed to reconnect to {host}:{port}: {e}")

        # If we have no peers left and there's a bootstrap, try to reconnect to bootstrap
        if not self.peers and self.bootstrap:
            print("[RECONNECT] No peers left, trying bootstrap node")
            threading.Thread(
                target=self.connect_to_peer,
                args=(self.bootstrap[0], self.bootstrap[1]),
                daemon=True
            ).start()

    # --- helpers for CLI ---
    def show_peers(self):
        peers = self.network.get_peers()
        last_seen = self.network.last_seen
        clock_offsets = self.network.clock_offsets

        print("=== peers ===")
        print(f"DEBUG: Total peers in dictionary: {len(peers)}")
        print(f"DEBUG: Peer IDs: {list(peers.keys())}")
        for pid,(h,p) in peers.items():
            age = now() - last_seen.get(pid, 0) if last_seen.get(pid) else float("inf")
            off = clock_offsets.get(pid, None)
            leader_flag = " (LEADER)" if pid == self.leader_id else ""
            print(pid + leader_flag, "->", f"{h}:{p}", f"last={age:.2f}s", f"offset={off:.4f}" if off is not None else "offset=n/a")
        print("=============")

    def debug_status(self):
        """Debug method to show current status"""
        peers = self.network.get_peers()
        print("=== DEBUG STATUS ===")
        print(f"My ID: {self.id}")
        print(f"I am leader: {self.is_leader}")
        print(f"Current leader_id: {self.leader_id}")
        print(f"Known peers: {list(peers.keys())}")
        print(f"Bootstrap: {self.bootstrap}")
        if self.leader_id:
            if self.leader_id in peers:
                print(f"Leader connection: {peers[self.leader_id]}")
            else:
                print("⚠️  Leader ID known but not in peers!")
        else:
            print("⚠️  No leader ID set!")
        print("===================")

    def play_next(self, lead_delay=1.0):
        # Stop current playback first
        self.player.stop_immediate()

        # advance local pointer and instruct peers to play next song at a future absolute time
        track = self.player.next_track()
        if not track:
            print("[PLAY] no tracks")
            return
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def play_previous(self, lead_delay=1.0):
        # Stop current playback first
        self.player.stop_immediate()

        # Get previous track
        track = self.player.previous_track()
        if not track:
            print("[PLAY] no tracks")
            return
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def play_index(self, index, lead_delay=1.0):
        self.player.stop_immediate()

        track = self.player.play_index(index)
        if not track:
            return
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def pause_immediate(self):
        if not self.player.get_playback_state()['is_playing']:
            print("[PAUSE] not currently playing")
            return

        pause_time = now()
        if self.is_leader:
            self.broadcast_pause(pause_time)
        else:
            peers = self.network.get_peers()
            leader_peers = [pid for pid in peers.keys() if pid != self.id]
            if leader_peers:
                leader_id = leader_peers[0]  # simply select the first peer as leader
                leader_host, leader_port = peers[leader_id]
                try:
                    send_json_to_addr(leader_host, leader_port, {
                        "type": "PAUSE_REQUEST",
                        "sender_id": self.id,
                        "pause_time": pause_time
                    })
                    print("[PAUSE] sent pause request to leader")
                except Exception as e:
                    print(f"[PAUSE] failed to send request: {e}")
            else:
                print("[PAUSE] no leader found")
            self._pause_local()

    def resume_immediate(self):
        if self.player.get_playback_state()['is_playing']:
            print("[RESUME] already playing")
            return

        resume_time = now()
        if self.is_leader:
            self.broadcast_resume(resume_time)
        else:
            peers = self.network.get_peers()
            leader_peers = [pid for pid in peers.keys() if pid != self.id]
            if leader_peers:
                leader_id = leader_peers[0]  # simply select the first peer as leader
                leader_host, leader_port = peers[leader_id]
                try:
                    send_json_to_addr(leader_host, leader_port, {
                        "type": "RESUME_REQUEST",
                        "sender_id": self.id,
                        "resume_time": resume_time
                    })
                    print("[RESUME] sent resume request to leader")
                except Exception as e:
                    print(f"[RESUME] failed to send request: {e}")
            else:
                print("[RESUME] no leader found")
            self._resume_local()

    def stop_immediate(self):
        """Stop playback immediately"""
        if not self.player.get_playback_state()['is_playing']:
            print("[STOP] not currently playing")
            return

        stop_time = now()
        if self.is_leader:
            self.broadcast_stop(stop_time)
        else:
            peers = self.network.get_peers()
            if self.leader_id in peers:
                leader_host, leader_port = peers[self.leader_id]
                try:
                    send_json_to_addr(leader_host, leader_port, {
                        "type": "STOP_REQUEST",
                        "sender_id": self.id,
                        "stop_time": stop_time
                    })
                    print("[STOP] sent stop request to leader")
                except Exception as e:
                    print(f"[STOP] failed to send request to leader: {e}")
            else:
                print("[STOP] no leader found")
            self._stop_local()

    def schedule_pause(self, delay=1.0):
        pause_time = now() + delay
        if self.is_leader:
            self.broadcast_pause(pause_time)
        else:
            print("[PAUSE] only leader can schedule pauses")

    def schedule_resume(self, delay=1.0):
        resume_time = now() + delay
        if self.is_leader:
            self.broadcast_resume(resume_time)
        else:
            print("[RESUME] only leader can schedule resumes")

    def list_tracks(self):
        """List available tracks"""
        return self.player.get_playlist()

    # Property accessors for compatibility with existing code
    @property
    def is_leader(self):
        return self.election.is_leader

    @is_leader.setter
    def is_leader(self, value):
        self.election.is_leader = value

    @property
    def leader_id(self):
        return self.election.leader_id

    @leader_id.setter
    def leader_id(self, value):
        self.election.leader_id = value

# --- CLI entrypoint ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("--bootstrap", nargs=2, metavar=("HOST","PORT"))
    parser.add_argument("--leader", action="store_true")
    args = parser.parse_args()

    bootstrap = tuple(args.bootstrap) if args.bootstrap else None
    node = PeerNode(args.host, args.port, bootstrap=bootstrap, is_leader=args.leader)
    node.start()

    # if bootstrap provided, do connect
    if bootstrap:
        # convert port to int
        threading.Thread(target=node.connect_to_peer, args=(bootstrap[0], int(bootstrap[1])), daemon=True).start()

    print("\nCommands:")
    print("  Network    : peers, sync, debug, force_election, discover")
    print("  Playback   : play <index>, pause, resume, stop, next, prev")
    print("  Scheduling : schedule_pause <delay>, schedule_resume <delay>")
    print("  Info       : list, status")
    print("  Control    : exit")
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
                            idx = int(cmd[1])
                            node.play_index(idx, lead_delay=1.0)
                        except ValueError:
                            print("Invalid index. Usage: play <index>")
                    else:
                        print("Usage: play <index>")

            elif cmd[0] == "sync":
                if not node.is_leader:
                    print("only leader runs sync in this demo")
                else:
                    node.sync_all_peers()

            elif cmd[0] == "list":
                # Updated to use the new list_tracks method
                tracks = node.list_tracks()
                if tracks:
                    print("Available tracks:")
                    for i, track in enumerate(tracks):
                        current_flag = " [CURRENT]" if i == node.player.current_index else ""
                        print(f"  {i}: {track}{current_flag}")
                else:
                    print("No tracks found in music directory")

            elif cmd[0] == "pause":
                node.pause_immediate()

            elif cmd[0] == "resume":
                node.resume_immediate()

            elif cmd[0] == "schedule_pause":
                if len(cmd) > 1:
                    try:
                        delay = float(cmd[1])
                        node.schedule_pause(delay)
                    except ValueError:
                        print("Invalid delay. Usage: schedule_pause <delay>")
                else:
                    print("Usage: schedule_pause <delay>")

            elif cmd[0] == "schedule_resume":
                if len(cmd) > 1:
                    try:
                        delay = float(cmd[1])
                        node.schedule_resume(delay)
                    except ValueError:
                        print("Invalid delay. Usage: schedule_resume <delay>")
                else:
                    print("Usage: schedule_resume <delay>")

            elif cmd[0] == "debug":
                node.debug_status()

            elif cmd[0] == "force_election":
                print("Forcing election...")
                node.elect_new_leader()

            elif cmd[0] == "status":
                # New command to show playback status
                playback_state = node.player.get_playback_state()
                print("=== PLAYBACK STATUS ===")
                print(f"Current track: {playback_state['current_track'] or 'None'}")
                print(f"Playing: {playback_state['is_playing']}")
                print(f"Current index: {playback_state['current_index']}")
                print(f"Pause position: {playback_state['pause_position']:.2f}s")
                if playback_state['is_playing']:
                    current_pos = node.player.get_current_position()
                    print(f"Current position: {current_pos:.2f}s")
                print("======================")

            elif cmd[0] == "stop":
                # New command to stop playback completely
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.stop_immediate()
                    print("Playback stopped")

            elif cmd[0] == "next":
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.play_next(lead_delay=1.0)

            elif cmd[0] == "prev":
                # Play previous track synchronized across all nodes
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.play_previous(lead_delay=1.0)

            elif cmd[0] == "discover":
                # Manual discovery command for debugging
                if node.peers:
                    first_peer = list(node.peers.items())[0]
                    pid, (h, p) = first_peer
                    print(f"Manually sending DISCOVERY_REQUEST to {pid} at {h}:{p}")
                    send_json_to_addr(h, p, {
                        "type": "DISCOVERY_REQUEST",
                        "sender_id": node.id
                    })
                else:
                    print("No peers to discover from")

            elif cmd[0] == "exit":
                node.player.stop_immediate()  # Use the new stop method
                break

            else:
                print("unknown command. Available commands:")
                print("  peers, playnext, play <index>, sync, list, pause, resume")
                print("  schedule_pause <delay>, schedule_resume <delay>, debug")
                print("  force_election, status, stop, next, prev, discover, exit")

    except KeyboardInterrupt:
        pass
    finally:
        node.running = False
        time.sleep(0.2)
        print("bye")

if __name__ == "__main__":
    main()