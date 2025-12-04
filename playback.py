# playback.py
import os
import time
import threading
import pygame

AUDIO_BACKEND = "pygame"
MUSIC_DIR = "music"

def now():
    return time.time()

class AudioPlayer:
    def __init__(self, music_dir=MUSIC_DIR):
        self.music_dir = music_dir

        # Playback state
        self.playlist = self._scan_music()
        self.current_index = 0
        self.is_playing = False
        self.pause_position = 0.0
        self.play_start_time = 0.0
        self.current_track = None

        # Lock for thread safety
        self.lock = threading.Lock()

        if AUDIO_BACKEND == "pygame":
            pygame.mixer.init()

    def _scan_music(self):
        if not os.path.isdir(self.music_dir):
            return []
        files = [f for f in os.listdir(self.music_dir) if f.lower().endswith((".mp3", ".wav", ".ogg"))]
        files.sort()
        return files

    def get_playlist(self):
        return self.playlist.copy()

    def get_playback_state(self):
        with self.lock:
            return {
                'is_playing': self.is_playing,
                'current_track': self.current_track,
                'current_index': self.current_index,
                'pause_position': self.pause_position,
                'play_start_time': self.play_start_time
            }

    def _start_play_local(self, track):
        local_path = os.path.join(self.music_dir, track) if track else None

        if not local_path or not os.path.isfile(local_path):
            print(f"[PLAY_ERR] Track file not found: {track}")
            return False

        # update status
        with self.lock:
            self.current_track = track
            self.is_playing = True
            self.play_start_time = now()
            self.pause_position = 0.0

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.music.load(local_path)
                pygame.mixer.music.play()

                time.sleep(0.05)
                if pygame.mixer.music.get_busy():
                    return True

            except Exception as e:
                print(f"[PLAY_ERR] Pygame failed: {e}")
        else:
            print(f"[PLAY_ERR] Pygame failed: {e}")

    def _start_play_local_from_position(self, track, position):
        local_path = os.path.join(self.music_dir, track) if track else None

        if not local_path or not os.path.isfile(local_path):
            print(f"[PLAY_ERR] Track file not found: {track}")
            return False

        if AUDIO_BACKEND == "pygame":
            try:
                if pygame.mixer.music.get_busy():
                    pygame.mixer.music.stop()

                pygame.mixer.music.load(local_path)
                pygame.mixer.music.play(start=position)

                with self.lock:
                    self.current_track = track
                    self.is_playing = True
                    self.play_start_time = now() - position
                    self.pause_position = 0.0

                time.sleep(0.05)
                if pygame.mixer.music.get_busy():
                    print(f"[RESUME] successfully resumed with pygame from {position:.2f}s")
                    return True
            except Exception as e:
                print(f"[RESUME_ERR] Pygame resume failed: {e}")

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
                else:
                    print("[PAUSE] pygame reports not playing")
            except Exception as e:
                print(f"[PAUSE_ERR] pygame pause failed: {e}")

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

        resume_position = self.pause_position

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.music.unpause()
                with self.lock:
                    self.is_playing = True
                    self.play_start_time = now() - self.resume_position
                    self.pause_position = 0.0
                print(f"[RESUME] pygame resumed from position: {resume_position:.2f}s")
                return
            except Exception as e:
                print(f"[RESUME_ERR] pygame resume failed: {e}")
                print(f"[RESUME] falling back to position-based playback")
                self._start_play_local_from_position(self.current_track, self.pause_position)
                return

    # --- Public API ---
    def prepare_and_schedule_play(self, track):
        local_path = os.path.join(self.music_dir, track) if track else None

        if local_path and os.path.isfile(local_path):
            if AUDIO_BACKEND == "pygame":
                try:
                    pygame.mixer.music.load(local_path)
                except Exception as e:
                    print("[AUDIO] load failed:", e)
            else:
                print("[Error] no pyname abckend")
                pass
        else:
            print("[AUDIO] track missing locally:", track)

        threading.Thread(target=self._delayed_play_thread, args=(track,), daemon=True).start()

    def _stop_local(self):
        with self.lock:
            if not self.is_playing and not self.current_track:
                print("[STOP] not currently playing, ignoring")
                return

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.music.stop()
                # print("[STOP] pygame music stopped")
            except Exception as e:
                print(f"[STOP_ERR] pygame stop failed: {e}")

        with self.lock:
            self.is_playing = False
            self.current_track = None
            self.pause_position = 0.0
            self.play_start_time = 0.0

    def _delayed_play_thread(self, track):
        # Sleep until target, then start
        time.sleep(0.5)
        self._start_play_local(track)

    def prepare_and_schedule_pause(self, delay):
        threading.Thread(target=self._delayed_pause_thread, args=(delay,), daemon=True).start()

    def _delayed_pause_thread(self, delay):
        time.sleep(0.5)
        self._pause_local()

    def prepare_and_schedule_resume(self, delay):
        threading.Thread(target=self._delayed_resume_thread, args=(delay,), daemon=True).start()

    def _delayed_resume_thread(self, delay):
        time.sleep(0.5)
        self._resume_local()

    def prepare_and_schedule_stop(self, delay):
        threading.Thread(target=self._delayed_stop_thread, args=(delay,), daemon=True).start()

    def _delayed_stop_thread(self, delay):
        time.sleep(0.5)
        self._stop_local()

    def next_track(self):
        """Go to next track in playlist"""
        with self.lock:
            if not self.playlist:
                return None
            self.current_index = (self.current_index + 1) % len(self.playlist)
            return self.playlist[self.current_index]

    def previous_track(self):
        """Go to previous track in playlist"""
        with self.lock:
            if not self.playlist:
                return None
            self.current_index = (self.current_index - 1) % len(self.playlist)
            return self.playlist[self.current_index]

    def play_index(self, index):
        """Play track at specific index"""
        with self.lock:
            if not self.playlist:
                return None
            self.current_index = index % len(self.playlist)
            self.current_track = self.playlist[self.current_index]
            return self.playlist[self.current_index]

    def get_current_position(self):
        """Get current playback position in seconds"""
        with self.lock:
            if self.is_playing:
                return now() - self.play_start_time
            else:
                return self.pause_position