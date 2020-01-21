import os
import logging.config
from pathlib import Path


def configure_logging(device_id: str):
    log_folder = Path.cwd() / "log"
    if not log_folder.parent.exists():
        log_folder.parent.mkdir(parents=True)

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "default": {"format": "%(asctime)s\t%(threadName)s\t%(levelname)s\t%(message)s"},
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
