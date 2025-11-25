# playback.py
import os
import sys
import subprocess
import time
import threading

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

def now():
    return time.time()

class AudioPlayer:
    def __init__(self, music_dir=MUSIC_DIR):
        self.music_dir = music_dir
        self._system_player = None

        # Playback state
        self.playlist = self._scan_music()
        self.current_index = 0
        self.is_playing = False
        self.pause_position = 0.0
        self.play_start_time = 0.0
        self.current_track = None

        # Lock for thread safety
        self.lock = threading.Lock()

        # Initialize audio backend
        if AUDIO_BACKEND == "pygame":
            pygame.mixer.init()

        print(f"[AUDIO] Backend: {AUDIO_BACKEND}")
        print(f"[AUDIO] Found {len(self.playlist)} tracks")

    def _scan_music(self):
        if not os.path.isdir(self.music_dir):
            return []
        files = [f for f in os.listdir(self.music_dir) if f.lower().endswith((".mp3", ".wav", ".ogg"))]
        files.sort()
        return files

    def get_playlist(self):
        return self.playlist.copy()

    def get_current_track(self):
        return self.current_track

    def get_playback_state(self):
        with self.lock:
            return {
                'is_playing': self.is_playing,
                'current_track': self.current_track,
                'current_index': self.current_index,
                'pause_position': self.pause_position,
                'play_start_time': self.play_start_time
            }

    def _play_with_system_command(self, local_path):
        """Fallback to system audio player"""
        try:
            if sys.platform == "darwin":  # macOS
                self._system_player = subprocess.Popen(['afplay', local_path],
                                                     stdout=subprocess.DEVNULL,
                                                     stderr=subprocess.DEVNULL)
            elif sys.platform == "linux":  # Linux
                self._system_player = subprocess.Popen(['aplay', local_path],
                                                     stdout=subprocess.DEVNULL,
                                                     stderr=subprocess.DEVNULL)
            elif sys.platform == "win32":  # Windows
                self._system_player = subprocess.Popen(['ffplay', '-nodisp', '-autoexit', local_path],
                                                     stdout=subprocess.DEVNULL,
                                                     stderr=subprocess.DEVNULL)
            else:
                print("[PLAY_ERR] Unsupported platform for system audio")
                return False
            print("[PLAY] started with system command")
            return True
        except Exception as e:
            print(f"[PLAY_ERR] System command failed: {e}")
            return False

    def _start_play_local(self, track):
        local_path = os.path.join(self.music_dir, track) if track else None

        if not local_path or not os.path.isfile(local_path):
            print(f"[PLAY_ERR] Track file not found: {track}")
            return False

        self._pause_local()

        # update status
        with self.lock:
            self.current_track = track
            self.is_playing = True
            self.play_start_time = now()
            self.pause_position = 0.0

        print(f"[DEBUG] Starting playback for: {track}")

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.quit()
                time.sleep(0.1)
                pygame.mixer.init(frequency=44100, size=-16, channels=2, buffer=2048)

                pygame.mixer.music.load(local_path)
                pygame.mixer.music.play()

                time.sleep(0.3)
                if pygame.mixer.music.get_busy():
                    print(f"[PLAY] successfully started with pygame: {track}")
                    return True
                else:
                    print("[PLAY_ERR] Pygame: Music loaded but not playing")
                    return self._play_with_system_command(local_path)

            except Exception as e:
                print(f"[PLAY_ERR] Pygame failed: {e}")
                return self._play_with_system_command(local_path)
        else:
            # use system command if pygame non exist
            return self._play_with_system_command(local_path)

    def _start_play_local_from_position(self, track, position):
        local_path = os.path.join(self.music_dir, track) if track else None

        if not local_path or not os.path.isfile(local_path):
            print(f"[PLAY_ERR] Track file not found: {track}")
            return False

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.quit()
                time.sleep(0.1)
                pygame.mixer.init(frequency=44100, size=-16, channels=2, buffer=2048)

                pygame.mixer.music.load(local_path)
                pygame.mixer.music.play(start=position)

                with self.lock:
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

        print(f"[RESUME] using approximate positioning with system command")
        return self._start_play_local(track)

    def _pause_local(self):
        print(f"[PAUSE] pausing playback at local_time={now():.3f}")

        with self.lock:
            if not self.is_playing:
                print("[PAUSE] not currently playing, ignoring")
                return

        if AUDIO_BACKEND == "pygame":
            try:
                if pygame.mixer.music.get_busy():
                    pygame.mixer.music.pause()
                    print("[PAUSE] pygame music paused")
                else:
                    print("[PAUSE] pygame reports not playing")
            except Exception as e:
                print(f"[PAUSE_ERR] pygame pause failed: {e}")

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

        with self.lock:
            if self.is_playing:
                elapsed = now() - self.play_start_time
                self.pause_position = elapsed
                self.is_playing = False
                print(f"[PAUSE] saved position: {self.pause_position:.2f}s")

    def _resume_local(self):
        print(f"[RESUME] resuming playback at local_time={now():.3f}")

        with self.lock:
            if self.is_playing:
                print("[RESUME] already playing")
                return

            if not self.current_track:
                print("[RESUME_ERR] no track to resume")
                return

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.music.unpause()
                with self.lock:
                    self.is_playing = True
                    self.play_start_time = now() - self.pause_position
                print(f"[RESUME] pygame resumed from position: {self.pause_position:.2f}s")
                return
            except Exception as e:
                print(f"[RESUME_ERR] pygame resume failed: {e}")
                print(f"[RESUME] falling back to position-based playback")
                self._start_play_local_from_position(self.current_track, self.pause_position)
                return

        if hasattr(self, '_system_player') and self._system_player is None and self.pause_position > 0:
            print(f"[RESUME] restarting system player from {self.pause_position:.2f}s")
            self._start_play_local_from_position(self.current_track, self.pause_position)
            return

        print("[RESUME] restarting from beginning")
        self._start_play_local(self.current_track)

    # --- Public API ---

    def prepare_and_schedule_play(self, track, delay):
        # Preload: load file to reduce startup jitter
        local_path = os.path.join(self.music_dir, track) if track else None
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

    def prepare_and_schedule_pause(self, delay):
        if delay <= 0:
            self._pause_local()
        else:
            threading.Thread(target=self._delayed_pause_thread, args=(delay,), daemon=True).start()

    def _delayed_pause_thread(self, delay):
        time.sleep(delay)
        self._pause_local()

    def prepare_and_schedule_resume(self, delay):
        if delay <= 0:
            self._resume_local()
        else:
            threading.Thread(target=self._delayed_resume_thread, args=(delay,), daemon=True).start()

    def _delayed_resume_thread(self, delay):
        time.sleep(delay)
        self._resume_local()

    def prepare_and_schedule_stop(self, delay):
        """Schedule stop for future synchronization"""
        if delay <= 0:
            self.stop_immediate()
        else:
            threading.Thread(target=self._delayed_stop_thread, args=(delay,), daemon=True).start()

    def _delayed_stop_thread(self, delay):
        """Thread for delayed stop"""
        time.sleep(delay)
        self.stop_immediate()

    def play_immediate(self, track):
        """Play a track immediately"""
        return self._start_play_local(track)

    def pause_immediate(self):
        """Pause playback immediately"""
        self._pause_local()

    def resume_immediate(self):
        """Resume playback immediately"""
        self._resume_local()

    def stop_immediate(self):
        """Stop playback immediately"""
        self._stop_local()

    def _stop_local(self):
        """Stop playback immediately (local only)"""
        print(f"[STOP] stopping playback at local_time={now():.3f}")

        with self.lock:
            if not self.is_playing and not self.current_track:
                print("[STOP] not currently playing, ignoring")
                return

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.music.stop()
                print("[STOP] pygame music stopped")
            except Exception as e:
                print(f"[STOP_ERR] pygame stop failed: {e}")

        if hasattr(self, '_system_player') and self._system_player:
            try:
                print(f"[STOP] terminating system player process {self._system_player.pid}")
                self._system_player.terminate()
              