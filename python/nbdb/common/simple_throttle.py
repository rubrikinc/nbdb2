"""
SimpleThrottle
"""
import time


# This class will get extended to handle more corner cases so
# likely new public methods will emerge
# pylint: disable-msg=R0903 # Too Few Public Methods
class SimpleThrottle:
    """
    Simple Throttle class provides a way to rate_limit operations for a single
    thread
    """

    def __init__(self,
                 rate_limit: int,
                 sleep_time: int,
                 rate_reset_time: int) -> None:
        """
        Initialize
        :param rate_limit: Rate limit in items/second
        :param sleep_time: Time to sleep when rate limit is exceeded, smaller
               values ensure that rate is uniform but overhead is higher
        :param rate_reset_time: The computed rate is reset after this time
        """
        self.count = 0
        self.instant_rate = 0
        self.start = time.time()
        self.rate_limit = rate_limit
        self.sleep_time = sleep_time
        self.rate_reset_time = rate_reset_time

    def throttle(self, item_count: int) -> bool:
        """
        Assumes we are going to process item_count in one go, computes are new
        instant_rate and if it exceeds
        the rate_limit then blocks the call for sleep_time
        :param item_count:
        :return: true if the call was throttled
        """
        throttled = False
        duration = self._reset()
        self.count = self.count + item_count
        if duration > 0:
            self.instant_rate = self.count/duration
            while self.instant_rate > self.rate_limit:
                time.sleep(self.sleep_time)
                throttled = True
                self.instant_rate = self.count/(time.time()-self.start)

        return throttled

    def _reset(self) -> int:
        """
        Checks the duration since last check, if it has exceeded the
        rate_reset_time then counts are reset
        and start is recorded from current time
        :return: duration since the count started, returns 0 if the count
                 has been reset
        """
        duration = time.time()-self.start
        if duration > self.rate_reset_time:
            self.count = 0
            self.start = time.time()
            return 0
        return duration
