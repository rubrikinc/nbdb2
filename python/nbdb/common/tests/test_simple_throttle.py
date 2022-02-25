"""
TestSimpleThrottle
"""
import time
from unittest import TestCase
from nbdb.common.simple_throttle import SimpleThrottle


class TestSimpleThrottle(TestCase):
    """
    Tests the SimpleThrottle by simulating a workload and verifying that
    workload is throttled at the expected rate.
    Because this tests a capability that requires real-time evaluation
    so the test incurs some time to run and will always be flagged as
    slow
    """

    def setUp(self) -> None:
        """
        create the simple throttle
        :return:
        """
        self.simple_throttle = SimpleThrottle(rate_limit=40,
                                              sleep_time=0.01,
                                              rate_reset_time=1)

    def test_throttle(self):
        """
        test that throttling rate matches the actual rate
        :return:
        """
        items = 0
        start = time.time()
        print('Running TestSimpleThrottle, this takes 2 seconds')
        last_duration = 0
        for _ in range(80):
            batch_count = 1
            items = items + batch_count
            self.simple_throttle.throttle(batch_count)
            duration = int(time.time() - start)
            if duration > last_duration:
                print('TestSimpleThrottle...{} s / 2s'.format(duration))
                last_duration = duration

        duration = time.time() - start
        self.assertTrue(duration > 0, 'This should have taken non-zero time'
                                      ' with throttle')

        measured_rate = items/duration

        self.assertLessEqual(measured_rate, self.simple_throttle.rate_limit)
        self.assertGreater(measured_rate, self.simple_throttle.rate_limit/2)

        delta = measured_rate/self.simple_throttle.instant_rate
        delta_target = measured_rate/self.simple_throttle.rate_limit
        self.assertGreater(delta, 0.95)
        self.assertGreater(delta_target, 0.9)
