import logging

from lib.config import config
from job import job


if __name__ == "__main__":
    log_level = config.get("LOG_LEVEL")
    logging.getLogger().setLevel(logging.getLevelName(log_level))

    job.run()
