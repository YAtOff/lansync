import os
import logging.config
from pathlib import Path
import sys
import threading


def install_excepthook():
    original_excepthook = sys.excepthook

    def handle_exception(exception_type, value, traceback):
        logging.exception("Unhandled exception")
        original_excepthook(type, value, traceback)

    sys.excepthook = handle_exception

    install_thread_excepthook()


def install_thread_excepthook():
    """
    Workaround for `sys.excepthook` thread bug from:
    http://bugs.python.org/issue1230540

    Call once from the main thread before creating any threads.
    """

    init_original = threading.Thread.__init__

    def init(self, *args, **kwargs):

        init_original(self, *args, **kwargs)
        run_original = self.run

        def run_with_except_hook(*args2, **kwargs2):
            try:
                run_original(*args2, **kwargs2)
            except Exception:
                sys.excepthook(*sys.exc_info())

        self.run = run_with_except_hook

    threading.Thread.__init__ = init


def configure_logging(device_id: str):
    log_folder = Path.cwd() / "log"
    if not log_folder.parent.exists():
        log_folder.parent.mkdir(parents=True)

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "default": {
                "format": f"%(asctime)s {device_id} %(message)s",
                "datefmt": "%H:%M:%S"
            },
            "stats": {"format": "%(message)s"}
        },
        "handlers": {
            "stats": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "filename": os.fspath(log_folder / f"stats-{device_id}.json"),
                "encoding": "utf-8",
            },
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "default"
            },
        },
        "loggers": {
            "stats": {
                "level": "INFO",
                "handlers": ["stats"],
                "propagate": False,
            },
        },
        "root": {"handlers": ["console"], "level": "INFO"},
    }

    logging.config.dictConfig(config)  # NOQA
    install_excepthook()
