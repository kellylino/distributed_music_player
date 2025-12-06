# playback.py
"""
Audio Playback Module - Handles Local Audio Playback
This module manages audio playback using Pygame mixer.
It supports synchronized playback with delay scheduling.
"""

import os
import time
import pygame
import threading

MUSIC_DIR = "music" # Directory containing audio files
AUDIO_BACKEND = "pygame"

def now():
    """Helper function to get current timestamp."""
    return time.time()

class AudioPlayer:
    """
    Manages local audio playback with synchronization capabilities.

    Features:
    - Load and play audio files from music directory
    - Schedule playback with precise timing
    - Track playback state (playing, paused, position)
    - Thread-safe operations
    """

    def __init__(self, music_dir=MUSIC_DIR):
        """
        Initialize audio player.

        Args:
            music_dir (str): Directory containing audio files
        """

        self.music_dir = music_dir

        # Playback state
        self.playlist = self._scan_music() # List of available tracks
        self.current_index = 0 # Index in playlist
        self.pause_position = 0.0 # Position when paused (seconds)
        self.play_start_time = 0.0 # When current playback started
        self.is_playing = False # Playback status
        self.current_track = None # Currently loaded track filename

        self.lock = threading.Lock() # Lock for thread safety

        # Initialize audio backend
        if AUDIO_BACKEND == "pygame":
            pygame.mixer.init()

    def _scan_music(self):
        """
        Scan music directory for audio files.

        Returns:
            list: Sorted list of audio filenames
        """

        if not os.path.isdir(self.music_dir):
            return []
        # Supported audio formats
        files = [f for f in os.listdir(self.music_dir) if f.lower().endswith((".mp3", ".wav", ".ogg"))]
        files.sort() # Alphabetical order
        return files

    def get_playlist(self):
        """Return copy of playlist to avoid modification issues."""
        return self.playlist.copy()

    def get_playback_state(self):
        """
        Get current playback state.

        Returns:
            dict: Playback state including track, position, and status
        """

        with self.lock:
            return {
                'is_playing': self.is_playing,
                'current_track': self.current_track,
                'current_index': self.current_index,
                'pause_position': self.pause_position,
                'play_start_time': self.play_start_time
            }

    def _start_play_local(self, track):
        """
        Start playing a track from the beginning.

        Args:
            track (str): Filename of track to play

        Returns:
            bool: True if playback started successfully
        """

        with self.lock:
            self.current_track = track
            self.is_playing = True
            self.play_start_time = now()
            self.pause_position = 0.0

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.music.play()

                # Brief delay to check if playback started
                time.sleep(0.05)
                if pygame.mixer.music.get_busy():
                    return True
                else:
                    return False

            except Exception as e:
                print(f"[PLAY_ERR] Pygame failed: {e}")
                return False

        return False

    def _start_play_local_from_position(self, track, position):
        """
        Start playing a track from specific position.

        Args:
            track (str): Filename of track to play
            position (float): Position in seconds to start from

        Returns:
            bool: True if playback started successfully
        """

        local_path = os.path.join(self.music_dir, track) if track else None

        if AUDIO_BACKEND == "pygame":
            try:
                if pygame.mixer.music.get_busy():
                    pygame.mixer.music.stop()

                # Load and play from position
                pygame.mixer.music.load(local_path)
                pygame.mixer.music.play(start=position)

                with self.lock:
                    self.current_track = track
                    self.is_playing = True
                    self.play_start_time = now() - position
                    self.pause_position = 0.0

                # Verify playback started
                time.sleep(0.05)
                if pygame.mixer.music.get_busy():
                    print(f"[RESUME] successfully resumed with pygame from {position:.2f}s")
                    return True
            except Exception as e:
                print(f"[RESUME_ERR] Pygame resume failed: {e}")
                # Fall back to regular play
                return self._start_play_local(track)

        return False

    def _pause_local(self):
        """Pause current playback."""

        with self.lock:
            if not self.is_playing:
                return

        if AUDIO_BACKEND == "pygame":
            try:
                if pygame.mixer.music.get_busy():
                    pygame.mixer.music.pause()
            except Exception as e:
                print(f"[PAUSE_ERR] pygame pause failed: {e}")

        # Update payback state
        with self.lock:
            if self.is_playing:
                elapsed = now() - self.play_start_time
                # self.pause_position = elapsed
                self.is_playing = False
                print(f"[PAUSE] saved position: {elapsed:.2f}s")

    def _stop_local(self):
        """Stop playback completely and reset state."""

        with self.lock:
            if not self.is_playing and not self.current_track:
                return

        if AUDIO_BACKEND == "pygame":
            try:
                pygame.mixer.music.stop()
            except Exception as e:
                print(f"[STOP_ERR] pygame stop failed: {e}")

        with self.lock:
            self.is_playing = False
            self.current_track = None
            self.pause_position = 0.0
            self.play_start_time = 0.0

    def _resume_local(self):
        """Resume current playback."""
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
                    self.play_start_time = now() - resume_position
                    self.pause_position = 0.0
                return
            except Exception as e:
                print(f"[RESUME_ERR] pygame resume failed: {e}")
                self._start_play_local_from_position(self.current_track, self.pause_position)
                return

    # --- Public API Methods ---
    def prepare_and_schedule_play(self, track, delay):
        """
        Schedule playback to start after specified delay.

        Args:
            track (str): Track filename to play
            delay (float): Seconds to wait before starting playback
        """

        local_path = os.path.join(self.music_dir, track) if track else None

        # Pre-load the track
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

        # Schedule playback in separate thread
        threading.Thread(target=self._delayed_play_thread, args=(delay, track), daemon=True).start()

    def _delayed_play_thread(self, delay, track):
        """Thread function for delayed playback."""

        time.sleep(delay)
        self._start_play_local(track)

    def prepare_and_schedule_pause(self, delay):
        """Schedule pause after specified delay."""
        threading.Thread(target=self._delayed_pause_thread, args=(delay,), daemon=True).start()

    def _delayed_pause_thread(self, delay):
        """Thread function for delayed pause."""

        time.sleep(delay)
        self._pause_local()

    def prepare_and_schedule_resume(self, track, delay):
        """
        Schedule resume after specified delay.

        Args:
            track (str): Track to resume
            delay (float): Seconds to wait before resuming
        """
        local_path = os.path.join(self.music_dir, track) if track else None

        # Pre-load the track
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

        threading.Thread(target=self._delayed_resume_thread, args=(delay,), daemon=True).start()

    def _delayed_resume_thread(self, delay):
        """Thread function for delayed resume."""
        time.sleep(delay)
        self._start_play_local_from_position(self.current_track, self.pause_position)
        # self._resume_local()

    def prepare_and_schedule_stop(self, delay):
        """Schedule stop after specified delay."""

        threading.Thread(target=self._delayed_stop_thread, args=(delay,), daemon=True).start()

    def _delayed_stop_thread(self, delay):
        """Thread function for delayed stop."""

        time.sleep(delay)
        self._stop_local()

    # --- Playlist Navigation ---

    def next_track(self):
        """Advance to next track in playlist (circular)."""

        with self.lock:
            if not self.playlist:
                return None
            self.current_index = (self.current_index + 1) % len(self.playlist)
            return self.playlist[self.current_index]

    def previous_track(self):
        """Go to previous track in playlist (circular)."""
        with self.lock:
            if not self.playlist:
                return None
            self.current_index = (self.current_index - 1) % len(self.playlist)
            return self.playlist[self.current_index]

    def play_index(self, index):
        """Play track at specific index in playlist"""

        with self.lock:
            if not self.playlist:
                return None
            self.current_index = index % len(self.playlist)
            self.current_track = self.playlist[self.current_index]
            return self.playlist[self.current_index]

    def get_current_position(self):
        """Get current playback position in seconds"""

        # Returns current position if playing, pause position if paused
        with self.lock:
            if self.is_playing:
                return now() - self.play_start_time
            else:
                return self.pause_position