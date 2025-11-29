# global_time.py
import time
import threading

class GlobalTimeManager:
    def __init__(self, node_id, is_leader=False):
        self.node_id = node_id
        self.is_leader = is_leader
        self.offset = 0  # Clock offset for synchronization
        self.lock = threading.Lock()

    def get_global_time(self):
        """Get synchronized global time"""
        with self.lock:
            return time.time() + self.offset

    def sync_with_leader(self, leader_time):
        """Synchronize clock with leader using Cristian's algorithm"""
        with self.lock:
            current_time = time.time()
            # One-way delay estimation (simplified)
            self.offset = leader_time - current_time
            print(f"[CLOCK] {self.node_id} synced: offset={self.offset:.3f}s")

    def set_leader_offset(self, leader_time):
        """Set clock based on leader's time"""
        with self.lock:
            self.offset = leader_time - time.time()