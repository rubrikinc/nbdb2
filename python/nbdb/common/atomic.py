"""
atomic.py - provide atomic API/types
"""
import threading


class AtomicNumber:
    """AtomicNumber - typically used as atomic counter"""
    def __init__(self, init_val, lock=None):
        self.m_lock = lock if lock else threading.Lock()
        self.m_val = init_val

    def get_value(self):
        """Get num value"""
        return self.m_val

    def set_value(self, val):
        """Set num value"""
        with self.m_lock:
            self.m_val = val

    def add_value(self, delta):
        """Add delta to num value"""
        with self.m_lock:
            self.m_val += delta
            return self.m_val

    def sub_value(self, delta):
        """Subtract delta from num value"""
        with self.m_lock:
            self.m_val -= delta
            return self.m_val

    def increment_value(self):
        """Increment num value by 1"""
        with self.m_lock:
            self.m_val += 1
            return self.m_val

    def decrement_value(self):
        """Decrement num value by 1"""
        with self.m_lock:
            self.m_val -= 1
            return self.m_val
