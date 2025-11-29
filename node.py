# node.py
import time
import uuid
import random
import argparse
import threading

# from global_time import GlobalTimeManager
from playback import AudioPlayer
from election import BullyElection
from network import NetworkManager
from common import send_json_to_addr
from message_handler import MessageHandler

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
            on_peer_update_callback=self._on_peer_update,
            on_leader_update_callback=self._update_leader_info
        )
        self.election = BullyElection(
            node_id=self.id,
            peers=self.network.peers,
            get_peer_address=self.network.get_peer_address,
            is_leader=is_leader,
            leader_id=self.id if is_leader else None
        )
        self.message_handler = MessageHandler(
            self.id, self.network, self.election, self.player
        )

        # Set up election callbacks
        # self.election.on_new_leader = self._on_new_leader

        print(f"[INIT] id={self.id} host={host}:{port} leader={self.is_leader}")

    def _handle_message(self, msg, conn):
        self.message_handler.handle_message(msg, conn)

    # def _on_peer_update(self, action, peer_id):
    #     if action == "removed" and peer_id == self.leader_id:
    #         print(f"[LEADER] {peer_id} disconnected — starting election")
    #         delay = random.uniform(0.5, 2.0)
    #         threading.Timer(delay, self.elect_new_leader).start()

    def _update_leader_info(self, leader_id):
        """Update the leader information in the election module"""
        print(f"[LEADER_UPDATE] Setting leader_id to {leader_id}")
        self.election.leader_id = leader_id

    def _on_peer_update(self, action, peer_id):
        current_leader_id = self.leader_id
        print(f"[DEBUG] Peer update: {action} for {peer_id}, current leader: {current_leader_id}")
        print(f"[DEBUG] I am leader: {self.is_leader}")
        print(f"[DEBUG] Election module leader_id: {self.election.leader_id}")

        if action == "removed" and peer_id == current_leader_id:
            print(f"[LEADER] {peer_id} disconnected — starting election")
            delay = random.uniform(0.5, 2.0)
            threading.Timer(delay, self.elect_new_leader).start()
        elif action == "removed":
            print(f"[DEBUG] Peer {peer_id} removed, but it's not the leader (leader is {current_leader_id})")

    def _on_new_leader(self, leader_id):
        """Callback when a new leader is elected"""
        print(f"[New leader] elected: {leader_id}")
        self.leader_id = leader_id
        self.is_leader = (leader_id == self.id)

        if self.is_leader:
            print("[LEADER] I am the new leader!")
            # New leader responsibilities
            threading.Thread(target=self._announce_new_leadership, daemon=True).start()
            # threading.Thread(target=self.sync_all_peers, daemon=True).start()
            # Reconnect to all peers
            # threading.Thread(target=self._reconnect_as_leader, daemon=True).start()
            # Broadcast reconnect request
            threading.Thread(target=self.message_handler.broadcast_reconnect_request, daemon=True).start()
        else:
            print(f"[FOLLOWER] I am a follower, new leader: {self.leader_id}")
            # Ensure connection to new leader
            # self._ensure_connected_to_leader()

    def _get_peer_address(self, peer_id):
        """Helper method for election module to get peer addresses"""
        with self.lock:
            peers = self.network.get_peers()
            return peers.get(peer_id, (None, None))

    def elect_new_leader(self):
        """Delegate election to the election module"""
        self.election.start_election(on_new_leader_callback=self._on_new_leader)

    def _announce_new_leadership(self):
        def send_to_peer(pid, message):
            host, port = self.network.get_peer_address(pid)
            if host and port:
                message["host"] = self.host
                message["port"] = self.port
                try:
                    send_json_to_addr(host, port, message)
                    print(f"[ANNOUNCE] Sent COORDINATOR to {pid}")
                except Exception as e:
                    print(f"[ANNOUNCE] Failed to send to {pid}: {e}")
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

    def _discover_leader(self):
        """Discover leader through other peers"""
        peers = self.network.get_peers()
        for pid, (host, port) in peers.items():
            if pid == self.id:
                continue

            try:
                # print(f"[DISCOVER] Asking {pid} about leader {self.leader_id}")
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

    # --- server ---
    def start(self):
        self.network.start()

    # === Leader Broadcast commands ===
    def broadcast_play(self, track, start_time, leader_id):

        self.network.broadcast_message("PLAY_REQUEST", {
            "track": track,
            "start_time": start_time
        }, leader_id)

    def broadcast_pause(self, pause_time, leader_id):
        # print(f"[DEBUG] Broadcasting pause to peers")
        self.network.broadcast_message("PAUSE_REQUEST", {
            "pause_time": pause_time
        }, leader_id)

    def broadcast_resume(self, resume_time, leader_id):

        self.network.broadcast_message("RESUME_REQUEST", {
            "resume_time": resume_time
        }, leader_id)

    def broadcast_stop(self, stop_time, leader_id):
        """Broadcast stop command to all peers"""

        # print(f"[DEBUG] Broadcasting stop to peers")
        self.network.broadcast_message("STOP_REQUEST", {
            "stop_time": stop_time
        }, leader_id)

    # --- clock sync (optional) ---
    def sync_clock_with_peer(self, pid, host, port):
        """
        leader connects, sends request, receives follower_time and computes offset.
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
                # print(f"[CLOCK] {pid} rtt={rtt:.4f}s offset={offset:.4f}s")
        except Exception:
            pass

    def _periodic_clock_sync(self):
        while self.running:
            time.sleep(6.0)
            if self.is_leader:
                self.sync_all_peers()

    def sync_all_peers(self):
        peers = self.network.get_peers()
        threads = []
        for pid,(h,p) in peers.items():
            if pid == self.id: continue
            t = threading.Thread(target=self.sync_clock_with_peer, args=(pid,h,p), daemon=True)
            t.start(); threads.append(t)
        for t in threads:
            t.join(timeout=0.3)

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
        print("===================")

    def play_next(self, lead_delay=0.5):
        # Stop current playback first
        self.player.stop_immediate()

        # advance local pointer and instruct peers to play next song at a future absolute time
        track = self.player.next_track()
        if not track:
            print("[PLAY] no tracks")
            return
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def play_previous(self, lead_delay=0.5):
        # Stop current playback first
        self.player.stop_immediate()

        # Get previous track
        track = self.player.previous_track()
        if not track:
            print("[PLAY] no tracks")
            return
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def play_index(self, index, lead_delay=0.5):
        self.player.stop_immediate()

        track = self.player.play_index(index)
        if not track:
            print("[PLAY] no tracks")
            return
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def pause_immediate(self):
        if not self.player.get_playback_state()['is_playing']:
            print("[PAUSE] not currently playing")
            return

        pause_time = now()
        self.broadcast_pause(pause_time, leader_id=self.id)

    def resume_immediate(self):
        if self.player.get_playback_state()['is_playing']:
            print("[RESUME] already playing")
            return

        resume_time = now()
        self.broadcast_resume(resume_time, leader_id=self.id)

    def stop_immediate(self):
        """Stop playback immediately"""
        if not self.player.get_playback_state()['is_playing']:
            print("[STOP] not currently playing")
            return

        stop_time = now()
        self.broadcast_stop(stop_time, leader_id=self.id)

    # def schedule_pause(self, delay):
    #     pause_time = now() + delay
    #     self.broadcast_pause(pause_time, leader_id=self.id)

    # def schedule_resume(self, delay):
    #     resume_time = now() + delay
    #     self.broadcast_resume(resume_time, leader_id=self.id)

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
        threading.Thread(target=node.network.connect_to_peer, args=(bootstrap[0], int(bootstrap[1])), daemon=True).start()

    print("\nCommands:")
    print("  Network    : peers, debug")
    print("  Playback   : play <index>, pause, resume, stop, next, prev")
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
                            node.play_index(idx, lead_delay=0.5)
                        except ValueError:
                            print("Invalid index. Usage: play <index>")
                    else:
                        print("Usage: play <index>")

            elif cmd[0] == "list":
                tracks = node.list_tracks()
                if tracks:
                    print("Available tracks:")
                    for i, track in enumerate(tracks):
                        current_flag = " [CURRENT]" if i == node.player.current_index else ""
                        print(f"  {i}: {track}{current_flag}")
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

            # elif cmd[0] == "schedule_pause":
            #     if not node.is_leader:
            #         print("only leader can schedule")
            #     else:
            #         if len(cmd) > 1:
            #             try:
            #                 delay = float(cmd[1])
            #                 node.schedule_pause(delay)
            #             except ValueError:
            #                 print("Invalid delay. Usage: schedule_pause <delay>")
            #         else:
            #             print("Usage: schedule_pause <delay>")

            # elif cmd[0] == "schedule_resume":
            #     if not node.is_leader:
            #         print("only leader can schedule")
            #     else:
            #         if len(cmd) > 1:
            #             try:
            #                 delay = float(cmd[1])
            #                 node.schedule_resume(delay)
            #             except ValueError:
            #                 print("Invalid delay. Usage: schedule_resume <delay>")
            #         else:
            #             print("Usage: schedule_resume <delay>")

            elif cmd[0] == "debug":
                node.debug_status()

            elif cmd[0] == "status":
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
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.stop_immediate()
                    print("Playback stopped")

            elif cmd[0] == "next":
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.play_next(lead_delay=0.5)

            elif cmd[0] == "prev":
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.play_previous(lead_delay=0.5)

            elif cmd[0] == "exit":
                node.stop_immediate()
                break

            else:
                print("\nUnknown command")
                print("  Available commands:")
                print("  Network    : peers, debug")
                print("  Playback   : play <index>, pause, resume, stop, next, prev")
                print("  Info       : list, status")
                print("  Control    : exit")

    except KeyboardInterrupt:
        pass
    finally:
        node.running = False
        time.sleep(0.2)
        print("bye")

if __name__ == "__main__":
    main()