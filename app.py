import logging

from lib.config import config


def run_job():
    logging.info('Starting job...')


if __name__ == '__main__':
    log_level = config.get('LOG_LEVEL')
    logging.getLogger().setLevel(logging.getLevelName(log_level))

    run_job()
