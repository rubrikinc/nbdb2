"""
tdigest_store.py - provide a thread-safe store of T-Digest objects
"""
import threading
from typing import List, Dict

import tdigest


class TDigestSync:
    """TDigestSync - thread-safe wrapper for tdigest.TDigest"""
    def __init__(self, delta: float = -1.0, k: int = -1,
                 lock: threading.Lock = None):
        self.m_lock = threading.Lock() if lock is None else lock
        if delta < 0.0 and k < 0:  # i.e. use default delta and k
            self.m_tdigest = tdigest.TDigest()
        elif k < 0:  # i.e. use default k
            self.m_tdigest = tdigest.TDigest(delta)
        else:
            self.m_tdigest = tdigest.TDigest(delta, k)

    def update(self, x: float, w: int = 1) -> None:
        """Update the t-digest with value x and weight w"""
        with self.m_lock:
            self.m_tdigest.update(x, w)

    def get_percentiles(self, pct_list: List[float],
                        decimal_round: int = 2) -> List[float]:
        """Returns the percentiles in pct_list (0 <= k <= 100), or an empty
        list if tdigest is empty, i.e. no values were inserted yet"""
        pct_values = []
        with self.m_lock:
            if len(self.m_tdigest) == 0:  # tdigest is empty
                return pct_values
            for pct in pct_list:
                pct_values.append(self.m_tdigest.percentile(pct))
        return [round(x, decimal_round) for x in pct_values]

    def cdf(self, x: float, decimal_round: int = 1) -> float:
        """Returns the CDF for value x as percentage (i.e. [0,100]), or -1.0
        if tdigest is empty, i.e. no values were inserted yet"""
        cdf_x: float = -1.0
        with self.m_lock:
            if len(self.m_tdigest) == 0:  # tdigest is empty
                return cdf_x
            cdf_x = self.m_tdigest.cdf(x)
        return round(cdf_x, decimal_round + 2) * 100


class TDigestStore:
    """A store for T-Digest objects"""
    # TODO Use a RW lock to avoid contention between readers
    def __init__(self, delta: float = -1.0, k: int = -1):
        self.m_lock: threading.Lock = threading.Lock()
        self.m_tdigest_map: Dict[str, TDigestSync] = {}
        self.m_init_delta: float = delta
        self.m_init_k: int = k

    def update(self, tdigest_id: str, x: float, w: int = 1) -> None:
        """Update the t-digest with value x and weight w"""
        with self.m_lock:
            td_sync: TDigestSync = self.m_tdigest_map.get(tdigest_id)
            if td_sync is None:
                td_sync = self.m_tdigest_map[tdigest_id] = TDigestSync(
                    self.m_init_delta, self.m_init_k)
        td_sync.update(x, w)

    def get_percentiles(self, tdigest_id: str, pct_list: List[float],
                        decimal_round: int = 2) -> List[float]:
        """Returns the CDF for value x as percentage (i.e. [0,100]), or an empty
        list if tdigest_id is not in map"""
        with self.m_lock:
            td_sync: TDigestSync = self.m_tdigest_map.get(tdigest_id)
        return [] if td_sync is None else td_sync.get_percentiles(pct_list,
                                                                  decimal_round)

    def cdf(self, tdigest_id: str, x: float, decimal_round: int = 1) -> float:
        """Returns the CDF for value x, or -1 if tdigest_id is not in map"""
        with self.m_lock:
            td_sync: TDigestSync = self.m_tdigest_map.get(tdigest_id)
        return -1.0 if td_sync is None else td_sync.cdf(x, decimal_round)
