import argparse
import socket
import threading
import time
import uuid
import os
import sys
import subprocess

from common import send_json_to_addr, send_json_on_sock, recv_json_from_sock

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
PEER_TIMEOUT = 8.0
MONITOR_INTERVAL = 1.0

def now():
    return time.time()

class PeerNode:
    def __init__(self, host, port, bootstrap=None, is_leader=False):
        self.id = str(uuid.uuid4())
        self.host = host
        self.port = port
        self.bootstrap = bootstrap
        self.is_leader = is_leader

        self._system_player = None

        # peer_id -> (host, port)
        self.peers = {}
        # last seen timestamps
        self.last_seen = {}
        # simple clock offsets (leader->peer), optional
        self.clock_offsets = {}  # peer_id -> offset seconds (peer_time - leader_midpoint)

        self.running = True
        self.lock = threading.Lock()

        # playlist (local copy)
        self.playlist = self._scan_music()
        self.current_index = 0

        self.is_playing = False
        self.pause_position = 0.0
        self.play_start_time = 0.0
        self.current_track = None

        # init audio if pygame
        if AUDIO_BACKEND == "pygame":
            pygame.mixer.init()

        print(f"[INIT] id={self.id} host={host}:{port} leader={self.is_leader}")
        print(f"[MUSIC] found {len(self.playlist)} tracks")

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
        try:
            resp = send_json_to_addr(host, port, {"type":"HELLO", "sender_id": self.id, "host": self.host, "port": self.port})
            if resp and resp.get("type")=="HELLO":
                pid = resp.get("sender_id")
                with self.lock:
                    self.peers[pid] = (host, port)
                    self.last_seen[pid] = now()
                # ask for discovery
                send_json_to_addr(host, port, {"type":"DISCOVERY_REQUEST", "sender_id": self.id})
        except Exception:
            pass

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
        # print("[MSG]", m, "from", sid)
        if m == "HELLO":
            # reply hello with host/port
            send_json_on_sock(conn, {"type":"HELLO", "sender_id": self.id, "host": self.host, "port": self.port})
            with self.lock:
                self.peers[sid] = (msg.get("host"), msg.get("port"))
                self.last_seen[sid] = now()

        elif m == "DISCOVERY_REQUEST":
            with self.lock:
                plist = [{"peer_id": pid, "host": h, "port": p} for pid, (h,p) in self.peers.items()]
            send_json_on_sock(conn, {"type":"DISCOVERY_RESPONSE", "sender_id": self.id, "peers": plist})

        elif m == "DISCOVERY_RESPONSE":
            for e in msg.get("peers", []):
                pid, h, p = e["peer_id"], e["host"], e["port"]
                if pid == self.id: continue
                with self.lock:
                    known = pid in self.peers
                if not known:
                    threading.Thread(target=self.connect_to_peer, args=(h,p), daemon=True).start()

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

        else:
            # unknown
            pass

    # --- scheduling & playback ---
    def _prepare_and_schedule_play(self, track, delay):
        # Preload: load file to reduce startup jitter
        local_path = os.path.join(MUSIC_DIR, track) if track else None
        if local_path and os.path.isfile(local_path):
            # Preload: for pygame we'll just load before waiting
            if AUDIO_BACKEND == "pygame":
                try:
                    pygame.mixer.music.load(local_path)
                except Exception as e:
                    print("[AUDIO] load failed:", e)
            else:
                # pydub loads when actually playing
                pass
        else:
            print("[AUDIO] track missing locally:", track)

        # if delay negative or small negative, play immediately
        if delay <= 0:
            self._start_play_local(track)
        else:
            threading.Thread(target=self._delayed_play_thread, args=(track, delay), daemon=True).start()

    def _delayed_play_thread(self, track, delay):
        # Sleep until target, then start
        time.sleep(delay)
        self._start_play_local(track)

    def _start_play_local(self, track):
        local_path = os.path.join(MUSIC_DIR, track) if track else None

        if not local_path or not os.path.isfile(local_path):
            print(f"[PLAY_ERR] Track file not found: {track}")
            return False

        # 先停止任何现有的播放
        self._pause_local()

        # 更新播放状态
        self.current_track = track
        self.is_playing = True
        self.play_start_time = now()
        self.pause_position = 0.0

        print(f"[DEBUG] Starting playback for: {track}")

        # 优先使用 pygame，因为它支持暂停/恢复和定位
        if AUDIO_BACKEND == "pygame":
            try:
                # 重新初始化确保状态正确
                pygame.mixer.quit()
                time.sleep(0.1)
                pygame.mixer.init(frequency=44100, size=-16, channels=2, buffer=2048)

                # 加载并播放
                pygame.mixer.music.load(local_path)
                pygame.mixer.music.play()

                # 检查是否真的在播放
                time.sleep(0.3)
                if pygame.mixer.music.get_busy():
                    print(f"[PLAY] successfully started with pygame: {track}")
                    return True
                else:
                    print("[PLAY_ERR] Pygame: Music loaded but not playing")
                    # 回退到系统命令
                    return self._play_with_system_command(local_path)

            except Exception as e:
                print(f"[PLAY_ERR] Pygame failed: {e}")
                # 回退到系统命令
                return self._play_with_system_command(local_path)
        else:
            # 如果没有pygame，使用系统命令
            return self._play_with_system_command(local_path)

    def _start_play_local_from_position(self, track, position):
        """从指定位置开始播放"""
        local_path = os.path.join(MUSIC_DIR, track) if track else None

        if not local_path or not os.path.isfile(local_path):
            print(f"[PLAY_ERR] Track file not found: {track}")
            return False

        # 优先使用 pygame 进行精确定位
        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.quit()
                time.sleep(0.1)
                pygame.mixer.init(frequency=44100, size=-16, channels=2, buffer=2048)

                pygame.mixer.music.load(local_path)
                pygame.mixer.music.play(start=position)  # pygame 支持从指定位置开始

                self.current_track = track
                self.is_playing = True
                self.play_start_time = now() - position
                self.pause_position = 0.0

                time.sleep(0.3)
                if pygame.mixer.music.get_busy():
                    print(f"[RESUME] successfully resumed with pygame from {position:.2f}s")
                    return True
            except Exception as e:
                print(f"[RESUME_ERR] Pygame resume failed: {e}")

        # 如果pygame失败，使用系统命令的近似方案
        print(f"[RESUME] using approximate positioning with system command")
        return self._start_play_local(track)

    def _prepare_and_schedule_pause(self, delay):
        if delay <= 0:
            self._pause_local()
        else:
            threading.Thread(target=self._delayed_pause_thread, args=(delay,), daemon=True).start()

    def _delayed_pause_thread(self, delay):
        time.sleep(delay)
        self._pause_local()

    def _pause_local(self):
        print(f"[PAUSE] pausing playback at local_time={now():.3f}")

        if not self.is_playing:
            print("[PAUSE] not currently playing, ignoring")
            return

        # 优先处理 pygame
        if AUDIO_BACKEND == "pygame":
            try:
                if pygame.mixer.music.get_busy():
                    pygame.mixer.music.pause()
                    print("[PAUSE] pygame music paused")
                else:
                    print("[PAUSE] pygame reports not playing")
            except Exception as e:
                print(f"[PAUSE_ERR] pygame pause failed: {e}")

        # 然后处理系统命令播放器
        if hasattr(self, '_system_player') and self._system_player:
            try:
                print(f"[PAUSE] terminating system player process {self._system_player.pid}")
                self._system_player.terminate()
                self._system_player.wait(timeout=1.0)
                self._system_player = None
                print("[PAUSE] system player terminated successfully")
            except Exception as e:
                print(f"[PAUSE_ERR] failed to terminate system player: {e}")
                try:
                    self._system_player.kill()
                    self._system_player = None
                except:
                    pass

        # 更新播放状态
        if self.is_playing:
            elapsed = now() - self.play_start_time
            self.pause_position = elapsed
            self.is_playing = False
            print(f"[PAUSE] saved position: {self.pause_position:.2f}s")

    def _prepare_and_schedule_resume(self, delay):
        if delay <= 0:
            self._resume_local()
        else:
            threading.Thread(target=self._delayed_resume_thread, args=(delay,), daemon=True).start()

    def _delayed_resume_thread(self, delay):
        time.sleep(delay)
        self._resume_local()

    def _resume_local(self):
        print(f"[RESUME] resuming playback at local_time={now():.3f}")

        if self.is_playing:
            print("[RESUME] already playing")
            return

        if not self.current_track:
            print("[RESUME_ERR] no track to resume")
            return

        # 首先尝试使用 pygame 恢复（如果之前是用 pygame 播放的）
        if AUDIO_BACKEND == "pygame":
            try:
                # 检查 pygame 是否已经加载了音乐
                pygame.mixer.music.unpause()
                self.is_playing = True
                # 更新开始时间以反映从暂停位置继续
                self.play_start_time = now() - self.pause_position
                print(f"[RESUME] pygame resumed from position: {self.pause_position:.2f}s")
                return
            except Exception as e:
                print(f"[RESUME_ERR] pygame resume failed: {e}")
                # 如果 pygame 恢复失败，尝试重新从位置开始播放
                print(f"[RESUME] falling back to position-based playback")
                self._start_play_local_from_position(self.current_track, self.pause_position)
                return

        # 如果 pygame 不可用，使用系统命令
        if hasattr(self, '_system_player') and self._system_player is None and self.pause_position > 0:
            print(f"[RESUME] restarting system player from {self.pause_position:.2f}s")
            self._start_play_local_from_position(self.current_track, self.pause_position)
            return

        # 最后的手段：重新开始播放
        print("[RESUME] restarting from beginning")
        self._start_play_local(self.current_track)

    def _start_play_local_from_position(self, track, position):
        """从指定位置开始播放"""
        local_path = os.path.join(MUSIC_DIR, track) if track else None

        if not local_path or not os.path.isfile(local_path):
            print(f"[PLAY_ERR] Track file not found: {track}")
            return False

        self.current_track = track
        self.is_playing = True
        self.play_start_time = now() - position
        self.pause_position = 0.0

        # 对于系统命令，需要支持从指定位置开始
        if sys.platform == "darwin":  # macOS
            try:
                # afplay 支持从指定时间开始
                self._system_player = subprocess.Popen([
                    'afplay', '-t', str(position), local_path
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print(f"[RESUME] restarted with afplay from {position:.2f}s")
                return True
            except Exception as e:
                print(f"[RESUME_ERR] afplay restart failed: {e}")

        # 其他平台或方法：重新开始播放（无法精确定位）
        print(f"[RESUME] restarting from beginning (positioning not supported)")
        return self._start_play_local(track)

    # --- leader utilities ---
    def broadcast_play(self, track, start_time, leader_id=None):
        # send PLAY_REQUEST to all known peers; ephemeral TCP
        if leader_id is None:
            leader_id = self.id
        with self.lock:
            peers_copy = dict(self.peers)
        for pid,(h,p) in peers_copy.items():
            try:
                send_json_to_addr(h, p, {"type":"PLAY_REQUEST", "sender_id": self.id, "leader_id": leader_id, "track": track, "start_time": start_time})
            except:
                pass
        # leader also plays locally at start_time
        local_delay = start_time - now()
        print(f"[LEADER] scheduled {track} at leader_time={start_time:.3f} (in {local_delay:.3f}s)")
        # schedule own playback
        if local_delay <= 0:
            self._start_play_local(track)
        else:
            threading.Thread(target=self._delayed_play_thread, args=(track, local_delay), daemon=True).start()

    def broadcast_pause(self, pause_time, leader_id=None):
        if leader_id is None:
            leader_id = self.id
        with self.lock:
            peers_copy = dict(self.peers)

        print(f"[DEBUG] Broadcasting pause to {len(peers_copy)} peers")

        for pid,(h,p) in peers_copy.items():
            try:
                send_json_to_addr(h, p, {"type":"PAUSE_REQUEST", "sender_id": self.id, "leader_id": leader_id, "pause_time": pause_time})
            except Exception as e:
                print(f"[DEBUG] Failed to send PAUSE_REQUEST to {pid}: {e}")

        # leader also pauses locally
        local_delay = pause_time - now()
        print(f"[LEADER] scheduled pause at leader_time={pause_time:.3f} (in {local_delay:.3f}s)")
        if local_delay <= 0:
            self._pause_local()
        else:
            threading.Thread(target=self._delayed_pause_thread, args=(local_delay,), daemon=True).start()

    def broadcast_resume(self, resume_time, leader_id=None):
        if leader_id is None:
            leader_id = self.id
        with self.lock:
            peers_copy = dict(self.peers)
        for pid,(h,p) in peers_copy.items():
            try:
                send_json_to_addr(h, p, {"type":"RESUME_REQUEST", "sender_id": self.id, "leader_id": leader_id, "resume_time": resume_time})
            except:
                pass
        # leader also resumes locally
        local_delay = resume_time - now()
        print(f"[LEADER] scheduled resume at leader_time={resume_time:.3f} (in {local_delay:.3f}s)")
        if local_delay <= 0:
            self._resume_local()
        else:
            threading.Thread(target=self._delayed_resume_thread, args=(local_delay,), daemon=True).start()

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
            with self.lock:
                for pid,last in list(self.last_seen.items()):
                    if pid == self.id: continue
                    if nowt - last > PEER_TIMEOUT:
                        removed.append(pid)
                        if pid in self.peers: del self.peers[pid]
                        if pid in self.last_seen: del self.last_seen[pid]
                        if pid in self.clock_offsets: del self.clock_offsets[pid]
            for pid in removed:
                print("[TIMEOUT] removed", pid)

    # --- helpers for CLI ---
    def show_peers(self):
        with self.lock:
            print("=== peers ===")
            for pid,(h,p) in self.peers.items():
                age = now() - self.last_seen.get(pid, 0) if self.last_seen.get(pid) else float("inf")
                off = self.clock_offsets.get(pid, None)
                print(pid, "->", f"{h}:{p}", f"last={age:.2f}s", f"offset={off:.4f}" if off is not None else "offset=n/a")
            print("=============")

    def play_next(self, lead_delay=1.0):
        # advance local pointer and instruct peers to play next song at a future absolute time
        if not self.playlist:
            print("[PLAY] no tracks")
            return
        self.current_index = (self.current_index + 1) % len(self.playlist)
        track = self.playlist[self.current_index]
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def play_index(self, index, lead_delay=1.0):
        if not self.playlist: return
        self.current_index = index % len(self.playlist)
        track = self.playlist[self.current_index]
        start_time = now() + lead_delay
        self.broadcast_play(track, start_time, leader_id=self.id)

    def pause_immediate(self):
        """立即暂停播放"""
        if not self.is_playing:
            print("[PAUSE] not currently playing")
            return

        pause_time = now()
        if self.is_leader:
            # 如果是leader，广播暂停命令
            self.broadcast_pause(pause_time)
        else:
            # 如果是follower，向leader发送暂停请求
            with self.lock:
                leader_peers = [pid for pid in self.peers.keys() if pid != self.id]
                if leader_peers:
                    leader_id = leader_peers[0]  # 简单选择第一个peer作为leader
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
            # 同时本地也暂停
            self._pause_local()

    def resume_immediate(self):
        """立即恢复播放"""
        if self.is_playing:
            print("[RESUME] already playing")
            return

        resume_time = now()
        if self.is_leader:
            # 如果是leader，广播恢复命令
            self.broadcast_resume(resume_time)
        else:
            # 如果是follower，向leader发送恢复请求
            with self.lock:
                leader_peers = [pid for pid in self.peers.keys() if pid != self.id]
                if leader_peers:
                    leader_id = leader_peers[0]  # 简单选择第一个peer作为leader
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
            # 同时本地也恢复
            self._resume_local()

    def schedule_pause(self, delay=1.0):
        """调度在指定延迟后暂停"""
        pause_time = now() + delay
        if self.is_leader:
            self.broadcast_pause(pause_time)
        else:
            print("[PAUSE] only leader can schedule pauses")

    def schedule_resume(self, delay=1.0):
        """调度在指定延迟后恢复"""
        resume_time = now() + delay
        if self.is_leader:
            self.broadcast_resume(resume_time)
        else:
            print("[RESUME] only leader can schedule resumes")

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

    print("Commands: peers | playnext | play <index> | sync | list | pause | resume | schedule_pause <delay> | schedule_resume <delay> | exit")
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
                    idx = int(cmd[1]) if len(cmd)>1 else 0
                    node.play_index(idx, lead_delay=1.0)
            elif cmd[0] == "sync":
                if not node.is_leader:
                    print("only leader runs sync in this demo")
                else:
                    node.sync_all_peers()
            elif cmd[0] == "list":
                print(node.playlist)
            elif cmd[0] == "pause":
                node.pause_immediate()
            elif cmd[0] == "resume":
                node.resume_immediate()
            elif cmd[0] == "schedule_pause":
                delay = float(cmd[1]) if len(cmd) > 1 else 1.0
                node.schedule_pause(delay)
            elif cmd[0] == "schedule_resume":
                delay = float(cmd[1]) if len(cmd) > 1 else 1.0
                node.schedule_resume(delay)
            elif cmd[0] == "exit":
                node.pause_immediate()
                break
            else:
                print("unknown")
    except KeyboardInterrupt:
        pass
    finally:
        node.running = False
        time.sleep(0.2)
        print("bye")

if __name__ == "__main__":
    main()
