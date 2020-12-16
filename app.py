import logging

from lib.config import config
from job import main


if __name__ == "__main__":
    log_level = config.get("LOG_LEVEL")
    logging.getLogger().setLevel(logging.getLevelName(log_level))

    main.run()
