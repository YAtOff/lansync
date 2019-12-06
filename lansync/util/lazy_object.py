class LazyObjectAlreadyConfiguredError(Exception):
    pass


class LazyObjectNotConfiguredError(Exception):
    pass


class LazyObject:

    _instance = None

    def __init__(self, reconfigure_enabled=False):
        self.reconfigure_enabled = reconfigure_enabled

    def configure(self, instance):
        if self._instance is not None and not self.reconfigure_enabled:
            raise LazyObjectAlreadyConfiguredError
        self._instance = instance

    def __getattr__(self, name):
        if self._instance is None:
            raise LazyObjectNotConfiguredError
        return getattr(self._instance, name)
