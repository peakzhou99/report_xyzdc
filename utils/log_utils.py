import os
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")


def get_logger():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    logger = logging.getLogger("ctz")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        handler = TimedRotatingFileHandler(filename=f"{LOG_DIR}/ctz.log", when="midnight", interval=1, backupCount=14,
                                           encoding="utf-8")
        handler.suffix = "%Y-%m-%d"
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - PID:%(process)d - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


if __name__ == "__main__":
    logger = get_logger()
    logger.info(LOG_DIR)
    logger.info(LOG_DIR)
    logger.info(LOG_DIR)
