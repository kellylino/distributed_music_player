# node.py
import argparse
import threading
import time
import uuid
from network import NetworkManager
from message_handler import MessageHandler
from election import BullyElection
from playback import AudioPlayer

# choose audio backend: try pygame first, else pydub
AUDIO_BACKEND = None
try:
    import pygame
    AUDIO_BACKEND = "pygame"
except Exception:
    try:
        from pydub import AudioSegment
        from pydub.playback import _play_with_simpleaudio as play_audio_pydub
        AUDIO_BACKEND = "pydub"
    except Exception:
        AUDIO_BACKEND = None

MUSIC_DIR = "music"
HEARTBEAT_INTERVAL = 2.0
PEER_TIMEOUT = 4.0
MONITOR_INTERVAL = 1.0
SYNC_INTERVAL = 4.0

def now():
    return time.time()

class PeerNode:
    def __init__(self, host, port, bootstrap=None, is_leader=False):
        self.id = str(uuid.uuid4())
        self.host = host
        self.port = port
        self.bootstrap = bootstrap

        # Network state
        self._system_player = None
        self.peers = {}  # peer_id -> (host, port)
        self.last_seen = {}  # last seen timestamps
        self.clock_offsets = {}  # peer_id -> offset seconds
        self.running = True
        self.lock = threading.Lock()

        # Initialize election module
        self.election = BullyElection(
            node_id=self.id,
            peers=self.peers,  # Reference to peers dict
            get_peer_address=self._get_peer_address,
            is_leader=is_leader,
            leader_id=self.id if is_leader else None
        )

        # Initialize playback module
        self.player = AudioPlayer()

        print(f"[INIT] id={self.id} host={host}:{port} leader={self.is_leader}")

    def _get_peer_address(self, peer_id):
        """Helper method for election module to get peer addresses"""
        with self.lock:
            return self.peers.get(peer_id, (None, None))

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
        else:
            print(f"[CALLBACK] I am a follower, new leader: {leader_id}")
            # Ensure connection to new leader
            self._ensure_connected_to_leader()

    def elect_new_leader(self):
        """Delegate election to the election module"""
        self.election.start_election(on_new_leader_callback=self._on_new_leader)

    def _announce_new_leadership(self):
        """Announce leadership using election module"""
        def send_to_peer(pid, message):
            if pid in self.peers:
                host, port = self.peers[pid]
                message["host"] = self.host
                message["port"] = self.port
                try:
                    send_json_to_addr(host, port, message)
                    print(f"[ANNOUNCE] Sent COORDINATOR to {pid}")
                except Exception as e:
                    print(f"[ANNOUNCE] Failed to send to {pid}: {e}")

        self.election.announce_leadership(send_to_peer_callback=send_to_peer)

    def _ensure_connected_to_leader(self):
        """Ensure we're connected to the current leader"""
        if not self.leader_id or self.leader_id == self.id:
            return

        with self.lock:
            if self.leader_id in self.peers:
                print(f"[CONNECT] Already connected to leader {self.leader_id}")
                return

            # Try to find leader in our known peers
            for pid, (host, port) in self.peers.items():
                if pid == self.leader_id:
                    print(f"[CONNECT] Found leader {self.leader_id} in known peers, connecting...")
                    threading.Thread(target=self.connect_to_peer, args=(host, port), daemon=True).start()
                    return

        # If leader not in peers, try to discover through other peers
        print(f"[CONNECT] Leader {self.leader_id} not in direct peers, discovering...")
        self._discover_leader()

    def _discover_leader(self):
        """Discover leader through other peers"""
        with self.lock:
            peers_copy = dict(self.peers)

        for pid, (host, port) in peers_copy.items():
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
                        threading.Thread(
                            target=self.connect_to_peer,
                            args=(leader_host, leader_port),
                            daemon=True
                        ).start()
                        return
            except Exception as e:
                print(f"[DISCOVER] Failed to get leader info from {pid}: {e}")

    def _scan_music(self):
        if not os.path.isdir(MUSIC_DIR):
            return []
        files = [f for f in os.listdir(MUSIC_DIR) if f.lower().endswith((".mp3", ".wav", ".ogg"))]
        files.sort()
        return files

    # --- server ---
    def start(self):
        threading.Thread(target=self._run_server, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        threading.Thread(target=self._network_sync_loop, daemon=True).start()

        # if bootstrap given, connect and discover peers
        if self.bootstrap:
            threading.Thread(target=self.connect_to_peer, args=(self.bootstrap[0], self.bootstrap[1]), daemon=True).start()

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
            self._handle_message(msg, conn)
        try:
            conn.close()
        except:
            pass

    # --- connect to peer (simple HELLO + discovery) ---
    def connect_to_peer(self, host, port):
        print(f"[CONNECT_DEBUG] Attempting to connect to {host}:{port}")
        try:
            resp = send_json_to_addr(host, port, {
                "type": "HELLO",
                "sender_id": self.id,
                "host": self.host,
                "port": self.port,
                "is_leader": self.is_leader,
                "leader_id": self.leader_id
            })
            if resp and resp.get("type") == "HELLO":
                pid = resp.get("sender_id")
                print(f"[CONNECT_DEBUG] Successfully connected to {pid} at {host}:{port}")

                # Update leader information
                if resp.get("is_leader"):
                    with self.lock:
                        self.leader_id = pid
                    print(f"[CONNECT] Connected to leader: {pid}")
                elif resp.get("leader_id"):
                    their_leader = resp.get("leader_id")
                    with self.lock:
                        if self.leader_id is None or their_leader > self.leader_id:
                            self.leader_id = their_leader
                            print(f"[CONNECT] Updated leader to: {their_leader} (from {pid})")

                with self.lock:
                    self.peers[pid] = (host, port)
                    self.last_seen[pid] = now()
                    print(f"[CONNECT_DEBUG] Added {pid} to peers list. Total peers now: {len(self.peers)}")

                # Ask for full discovery to get all peers
                send_json_to_addr(host, port, {
                    "type": "DISCOVERY_REQUEST",
                    "sender_id": self.id
                })

            else:
                print(f"[CONNECT_DEBUG] No valid HELLO response from {host}:{port}")
        except Exception as e:
            print(f"[CONNECT_DEBUG] Failed to connect to {host}:{port}: {e}")

    def _network_sync_loop(self):
        """Periodically sync with random peers to maintain complete network view"""
        while self.running:
            time.sleep(SYNC_INTERVAL)

            with self.lock:
                if not self.peers:
                    continue
                peers_list = list(self.peers.items())
                if peers_list:
                    random_peer = random.choice(peers_list)
                    pid, (h, p) = random_peer

            try:
                # print(f"[SYNC_DEBUG] Sending DISCOVERY_REQUEST to {pid} at {h}:{p}")

                # Send discovery request and get response
                response = send_json_to_addr(h, p, {
                    "type": "DISCOVERY_REQUEST",
                    "sender_id": self.id
                })

                # CRITICAL FIX: Process the discovery response if received
                if response and response.get("type") == "DISCOVERY_RESPONSE":
                    # print(f"[SYNC_DEBUG] Processing DISCOVERY_RESPONSE from {pid}")
                    # Create a mock connection object for the handler
                    class MockConn:
                        def __init__(self):
                            pass
                        def close(self):
                            pass

                    # Process the discovery response through the message handler
                    self._handle_message(response, MockConn())
                else:
                    print(f"[SYNC_DEBUG] No valid DISCOVERY_RESPONSE received from {pid}")

            except Exception as e:
                print(f"[SYNC_DEBUG] Failed to sync with {pid}: {e}")

    def _update_last_seen(self, msg):
        sid = msg.get("sender_id")
        if sid:
            with self.lock:
                self.last_seen[sid] = now()
                if "host" in msg and "port" in msg:
                    if sid not in self.peers:
                        self.peers[sid] = (msg["host"], msg["port"])

    def _handle_message(self, msg, conn):
        m = msg.get("type")
        sid = msg.get("sender_id")

        # TEMPORARY DEBUGGING - REMOVE LATER
        # print(f"[MSG_FLOW] Received message type: {m} from {sid}")

        # Handle election messages first
        if m in ["ELECTION", "COORDINATOR"]:
            def send_response(response_msg):
                send_json_on_sock(conn, response_msg)

            self.election.handle_election_message(msg, conn, send_response)
            return

        if m == "HELLO":
            # Reply with our complete information
            send_json_on_sock(conn, {
                "type": "HELLO",
                "sender_id": self.id,
                "host": self.host,
                "port": self.port,
                "is_leader": self.is_leader,
                "leader_id": self.leader_id
            })

            # Update our knowledge from the incoming HELLO
            with self.lock:
                self.peers[sid] = (msg.get("host"), msg.get("port"))
                self.last_seen[sid] = now()

                # Update leader information
                if msg.get("is_leader"):
                    if self.leader_id != sid:
                        self.leader_id = sid
                        print(f"[HELLO] Updated leader to: {sid}")
                elif msg.get("leader_id"):
                    their_leader = msg.get("leader_id")
                    if self.leader_id is None or their_leader > self.leader_id:
                        self.leader_id = their_leader
                        print(f"[HELLO] Updated leader to: {their_leader} (from {sid})")

        elif m == "DISCOVERY_REQUEST":
            # Share ALL known peers (not just direct connections)
            with self.lock:
                # Include self in the peer list
                plist = [{"peer_id": self.id, "host": self.host, "port": self.port, "is_leader": self.is_leader}]
                # Include all known peers
                for pid, (h, p) in self.peers.items():
                    plist.append({"peer_id": pid, "host": h, "port": p, "is_leader": (pid == self.leader_id)})

            # print(f"[DISCOVERY] Sharing {len(plist)} peers with {sid}")

            send_json_on_sock(conn, {
                "type": "DISCOVERY_RESPONSE",
                "sender_id": self.id,
                "peers": plist
            })

        elif m == "DISCOVERY_RESPONSE":
            # print(f"[DISCOVERY_DEBUG] Received peer list with {len(msg.get('peers', []))} entries from {sid}")

            # Debug: print all peers in the received message
            for i, e in enumerate(msg.get("peers", [])):
                pid = e["peer_id"]
                h = e["host"]
                p = e["port"]
                is_leader = e.get("is_leader", False)
                # print(f"[DISCOVERY_DEBUG] {i}: {pid} at {h}:{p} (leader: {is_leader})")

            new_peers = 0
            updated_peers = 0

            for e in msg.get("peers", []):
                pid, h, p = e["peer_id"], e["host"], e["port"]
                is_leader = e.get("is_leader", False)

                if pid == self.id:
                    # print(f"[DISCOVERY_DEBUG] Skipping self: {pid}")
                    continue

                with self.lock:
                    # Update leader information
                    if is_leader:
                        if self.leader_id != pid:
                            # print(f"[DISCOVERY_DEBUG] Setting leader to: {pid}")
                            self.leader_id = pid

                    # Check if this is a new peer or existing one
                    if pid not in self.peers:
                        new_peers += 1
                        # print(f"[DISCOVERY_DEBUG] NEW PEER: {pid} at {h}:{p}")
                    else:
                        updated_peers += 1
                        # print(f"[DISCOVERY_DEBUG] UPDATED PEER: {pid} at {h}:{p}")

                    # Always update the peer information
                    self.peers[pid] = (h, p)
                    self.last_seen[pid] = now()

            # print(f"[DISCOVERY_SUMMARY] New: {new_peers}, Updated: {updated_peers}, Total: {len(self.peers)}")

            # Now connect to any peers we're not actively connected to
            if new_peers > 0:
                print(f"[DISCOVERY] Establishing connections to {new_peers} new peers...")
                for e in msg.get("peers", []):
                    pid, h, p = e["peer_id"], e["host"], e["port"]
                    is_leader = e.get("is_leader", False)

                    # Skip self and leader (we're already connected to leader)
                    if pid == self.id or pid == self.leader_id:
                        continue

                    # Connect to non-leader peers
                    print(f"[DISCOVERY] Attempting connection to {pid} at {h}:{p}")
                    threading.Thread(target=self.connect_to_peer, args=(h, p), daemon=True).start()

        elif m == "LEADER_DISCOVERY":
            # Peer is asking about a leader
            requested_leader_id = msg.get("leader_id")

            if requested_leader_id == self.leader_id and self.leader_id in self.peers:
                # We know about this leader
                leader_host, leader_port = self.peers[self.leader_id]
                send_json_on_sock(conn, {
                    "type": "LEADER_INFO",
                    "sender_id": self.id,
                    "leader_id": self.leader_id,
                    "host": leader_host,
                    "port": leader_port
                })

        elif m == "LEADER_INFO":
            # Response to leader discovery request
            leader_host = msg.get("host")
            leader_port = msg.get("port")
            if leader_host and leader_port:
                print(f"[LEADER_INFO] Connecting to leader at {leader_host}:{leader_port}")
                threading.Thread(
                    target=self.connect_to_peer,
                    args=(leader_host, leader_port),
                    daemon=True
                ).start()

        elif m == "HEARTBEAT":
            # reply ack
            send_json_on_sock(conn, {"type":"HEARTBEAT_ACK", "sender_id": self.id})

        elif m == "PLAY_REQUEST":
            # leader tells us which track & absolute start_time (leader wall-clock)
            track = msg.get("track")
            start_time = msg.get("start_time")  # leader-walltime (seconds)
            leader_id = msg.get("leader_id") or sid
            # convert: local_time_target = start_time + offset (if we have offset leader->this peer)
            offset = self.clock_offsets.get(leader_id, 0.0)
            local_target = start_time + offset
            delay = local_target - now()
            print(f"[PLAY_REQ] leader={leader_id} track={track} start_time={start_time:.3f} local_target={local_target:.3f} delay={delay:.3f}s")
            # load and schedule
            self._prepare_and_schedule_play(track, delay)

        elif m == "PAUSE_REQUEST":
            # leader tells us to pause at absolute time
            pause_time = msg.get("pause_time")  # leader-walltime (seconds)
            leader_id = msg.get("leader_id") or sid
            # convert to local time
            offset = self.clock_offsets.get(leader_id, 0.0)
            local_target = pause_time + offset
            delay = local_target - now()
            print(f"[PAUSE_REQ] leader={leader_id} pause_time={pause_time:.3f} local_target={local_target:.3f} delay={delay:.3f}s")
            # schedule pause
            self._prepare_and_schedule_pause(delay)

        elif m == "RESUME_REQUEST":
            # leader tells us to resume at absolute time
            resume_time = msg.get("resume_time")  # leader-walltime (seconds)
            leader_id = msg.get("leader_id") or sid
            # convert to local time
            offset = self.clock_offsets.get(leader_id, 0.0)
            local_target = resume_time + offset
            delay = local_target - now()
            print(f"[RESUME_REQ] leader={leader_id} resume_time={resume_time:.3f} local_target={local_target:.3f} delay={delay:.3f}s")
            # schedule resume
            self._prepare_and_schedule_resume(delay)

        elif m == "NEXT_REQUEST":
            # leader wants to advance playlist and issue a new PLAY_REQUEST (optional; for P2P we might just use PLAY_REQUEST directly)
            pass

        elif m == "CLOCK_SYNC_REQUEST":
            # simple reply with follower_time
            follower_time = now()
            send_json_on_sock(conn, {"type":"CLOCK_SYNC_RESPONSE", "sender_id": self.id, "follower_time": follower_time})

        elif m == "CLOCK_SYNC_RESPONSE":
            # leader side: compute offset if needed (not used automatically here)
            # For simplicity we handle direct sync path in sync_clock_with_peer()
            pass

        elif m == "FULL_PEER_LIST":
            # Leader is sharing the complete peer list
            print(f"[PEER_DISCOVERY] Received full peer list from leader")
            new_peers = 0
            for peer_info in msg.get("peers", []):
                pid = peer_info["peer_id"]
                host = peer_info["host"]
                port = peer_info["port"]
                is_leader_peer = peer_info.get("is_leader", False)

                if pid == self.id:
                    continue

                with self.lock:
                    if pid not in self.peers:
                        self.peers[pid] = (host, port)
                        self.last_seen[pid] = now()
                        new_peers += 1

                        # If this peer is the leader, update leader_id
                        if is_leader_peer:
                            self.leader_id = pid
                            print(f"[PEER_DISCOVERY] Updated leader to {pid}")

            if new_peers > 0:
                print(f"[PEER_DISCOVERY] Discovered {new_peers} new peers")
                # Connect to newly discovered peers (except leader, since we're already connected)
                for peer_info in msg.get("peers", []):
                    pid = peer_info["peer_id"]
                    host = peer_info["host"]
                    port = peer_info["port"]
                    is_leader_peer = peer_info.get("is_leader", False)

                    if pid == self.id or (is_leader_peer and pid == self.leader_id):
                        continue

                    with self.lock:
                        if pid not in self.peers:  # Only connect if not already connected
                            threading.Thread(
                                target=self.connect_to_peer,
                                args=(host, port),
                                daemon=True
                            ).start()

        else:
            # unknown
            pass

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

    # --- leader utilities ---
    def broadcast_play(self, track, start_time, leader_id=None):
        # send PLAY_REQUEST to all known peers; ephemeral TCP
        if leader_id is None:
            leader_id = self.id
        with self.lock:
            peers_copy = dict(self.peers)
        for pid,(h,p) in peers_copy.items():
            try:
                send_json_to_addr(h, p, {
                    "type": "PLAY_REQUEST",
                    "sender_id": self.id,
                    "leader_id": leader_id,
                    "track": track,
                    "start_time": start_time
                })
            except:
                pass
        # leader also plays locally at start_time
        local_delay = start_time - now()
        print(f"[LEADER] scheduled {track} at leader_time={start_time:.3f} (in {local_delay:.3f}s)")
        # schedule own playback
        self._prepare_and_schedule_play(track, local_delay)

    def broadcast_pause(self, pause_time, leader_id=None):
        if leader_id is None:
            leader_id = self.id
        with self.lock:
            peers_copy = dict(self.peers)

        print(f"[DEBUG] Broadcasting pause to {len(peers_copy)} peers")

        for pid,(h,p) in peers_copy.items():
            try:
                send_json_to_addr(h, p, {
                    "type": "PAUSE_REQUEST",
                    "sender_id": self.id,
                    "leader_id": leader_id,
                    "pause_time": pause_time
                })
            except Exception as e:
                print(f"[DEBUG] Failed to send PAUSE_REQUEST to {pid}: {e}")

        # leader also pauses locally
        local_delay = pause_time - now()
        print(f"[LEADER] scheduled pause at leader_time={pause_time:.3f} (in {local_delay:.3f}s)")
        self._prepare_and_schedule_pause(local_delay)

    def broadcast_resume(self, resume_time, leader_id=None):
        if leader_id is None:
            leader_id = self.id
        with self.lock:
            peers_copy = dict(self.peers)
        for pid,(h,p) in peers_copy.items():
            try:
                send_json_to_addr(h, p, {
                    "type": "RESUME_REQUEST",
                    "sender_id": self.id,
                    "leader_id": leader_id,
                    "resume_time": resume_time
                })
            except:
                pass
        # leader also resumes locally
        local_delay = resume_time - now()
        print(f"[LEADER] scheduled resume at leader_time={resume_time:.3f} (in {local_delay:.3f}s)")
        self._prepare_and_schedule_resume(local_delay)

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
                    self.clock_offsets[pid] = offset
                    self.last_seen[pid] = now()
                print(f"[CLOCK] {pid} rtt={rtt:.4f}s offset={offset:.4f}s")
        except Exception:
            pass

    def sync_all_peers(self):
        with self.lock:
            peers_copy = dict(self.peers)
        threads = []
        for pid,(h,p) in peers_copy.items():
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
                for pid, last in list(self.last_seen.items()):
                    if pid == self.id:
                        continue
                    if nowt - last > PEER_TIMEOUT:
                        removed.append(pid)
                        if pid in self.peers:
                            del self.peers[pid]
                        if pid in self.last_seen:
                            del self.last_seen[pid]
                        if pid in self.clock_offsets:
                            del self.clock_offsets[pid]

                        # Check if the removed peer was the leader
                        if pid == self.leader_id:
                            leader_lost = True

            # Process removals
            for pid in removed:
                print("[TIMEOUT] removed", pid)

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
        with self.lock:
            for pid, (host, port) in self.peers.items():
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
        with self.lock:
            print("=== peers ===")
            print(f"DEBUG: Total peers in dictionary: {len(self.peers)}")
            print(f"DEBUG: Peer IDs: {list(self.peers.keys())}")
            for pid,(h,p) in self.peers.items():
                age = now() - self.last_seen.get(pid, 0) if self.last_seen.get(pid) else float("inf")
                off = self.clock_offsets.get(pid, None)
                leader_flag = " (LEADER)" if pid == self.leader_id else ""
                print(pid + leader_flag, "->", f"{h}:{p}", f"last={age:.2f}s", f"offset={off:.4f}" if off is not None else "offset=n/a")
            print("=============")

    def debug_status(self):
        """Debug method to show current status"""
        with self.lock:
            print("=== DEBUG STATUS ===")
            print(f"My ID: {self.id}")
            print(f"I am leader: {self.is_leader}")
            print(f"Current leader_id: {self.leader_id}")
            print(f"Known peers: {list(self.peers.keys())}")
            print(f"Bootstrap: {self.bootstrap}")
            if self.leader_id:
                if self.leader_id in self.peers:
                    print(f"Leader connection: {self.peers[self.leader_id]}")
                else:
                    print("⚠️  Leader ID known but not in peers!")
            else:
                print("⚠️  No leader ID set!")
            print("===================")

    def play_next(self, lead_delay=1.0):
        # advance local pointer and instruct peers to play next song at a future absolute time
        track = self.player.next_track()
        if not track:
            print("[PLAY] no tracks")
            return
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def play_index(self, index, lead_delay=1.0):
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
            with self.lock:
                leader_peers = [pid for pid in self.peers.keys() if pid != self.id]
                if leader_peers:
                    leader_id = leader_peers[0]  # simply select the first peer as leader
                    leader_host, leader_port = self.peers[leader_id]
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
            with self.lock:
                leader_peers = [pid for pid in self.peers.keys() if pid != self.id]
                if leader_peers:
                    leader_id = leader_peers[0]  # simply select the first peer as leader
                    leader_host, leader_port = self.peers[leader_id]
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
    print("  Playback   : playnext, play <index>, pause, resume, stop, next, prev")
    print("  Scheduling : schedule_pause <delay>, schedule_resume <delay>")
    print("  Info       : list, status")
    print("  Control    : exit")
    try:
        while True:
            cmd = input("> ").strip().split()
            if not cmd: continue

            if cmd[0] == "peers":
                node.show_peers()

            elif cmd[0] == "playnext":
                if not node.is_leader:
                    print("only leader can schedule global play")
                else:
                    node.play_next(lead_delay=1.0)

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
                node.player.stop_immediate()
                print("Playback stopped")

            elif cmd[0] == "next":
                # Play next track immediately (local only)
                track = node.player.next_track()
                if track:
                    print(f"Playing next track: {track}")
                    node.player.play_immediate(track)
                else:
                    print("No tracks available")

            elif cmd[0] == "prev":
                # Play previous track immediately (local only)
                track = node.player.previous_track()
                if track:
                    print(f"Playing previous track: {track}")
                    node.player.play_immediate(track)
                else:
                    print("No tracks available")

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