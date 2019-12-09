from threading import Timer
from typing import Callable


class Timeout:
    def __init__(self, action: Callable, interval=60):
        self.action = action
        self.interval = interval
        self.timer = None

    def start(self):
        self.timer = Timer(self.interval, self.action)
        self.timer.daemon = True
        self.timer.start()

    def stop(self):
        self.timer and self.timer.cancel()
        self.timer = None

    def restart(self):
        self.stop()
        self.start()
