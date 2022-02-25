"""
configuration synchronizer
"""
import threading
import time
import requests


class ConfigSynchronizer(threading.Thread):
    """
    The synchronizer periodically requests the configuration
    from the given endpoint in a daemon thread, and notify the
    application with new configuration

    Usage sample:
    config = ConfigSynchronizer.getInstance().subscribe(endpoint,
                            refresh_interval_sec, default_config,
                            validate_func, notify_func, app_logger)
    """

    __instance = None

    @staticmethod
    def getInstance():
        """
        Create a singleton of ConfigSynchronizer
        """
        if ConfigSynchronizer.__instance is None:
            ConfigSynchronizer.__instance = ConfigSynchronizer()
        return ConfigSynchronizer.__instance

    def __init__(self):
        """
        Initialize the instance
        """
        threading.Thread.__init__(self)
        self.endpoint = None
        self.refresh_interval_sec = None
        self.config = None
        self.validate_func = None
        self.notify_func = None
        self.app_logger = None

    def refresh_config(self, notify=True):
        """
        refresh configuration from the endpoint
        and notify the application
        """
        try:
            r = requests.get(self.endpoint, timeout=1)
            config = r.text
            if self.config == config:
                return
            if not self.validate_func(config):
                self.app_logger.error('config is invalid %s' % (str(config)))
                return
            self.app_logger.info('new config: %s' % (str(config)))
            # notify app of a new config
            if notify:
                self.notify_func(self.config, config)
            self.config = config
        except Exception as ex:
            self.app_logger.error(
                    'ConfigSynchronizer failed to sync config for endpoint %s: %s'
                    % (self.endpoint, str(ex)))

    def subscribe(self, endpoint, refresh_interval_sec, default_config,
                        validate_func, notify_func, app_logger):
        """
        Subscribe to the configuration
        """
        self.endpoint = endpoint
        self.refresh_interval_sec = refresh_interval_sec
        self.config = default_config
        self.validate_func = validate_func
        self.notify_func = notify_func
        self.app_logger = app_logger
        self.refresh_config(False)
        config = self.config
        self.start()
        return config

    def run(self):
        """
        Periodically request the configuration from the endpoint,
        and update the current snapshot of configuration
        """
        while True:
            time.sleep(self.refresh_interval_sec)
            self.refresh_config()
