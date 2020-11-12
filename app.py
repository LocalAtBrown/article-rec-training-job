import logging

from lib.config import config
from lib.db import test_query


def run_job():
    logging.info('Starting job...')
    logging.info('Running test query...')
    query_result = test_query()
    logging.info(f'Got query result: {query_result}')


if __name__ == '__main__':
    log_level = config.get('LOG_LEVEL')
    logging.getLogger().setLevel(logging.getLevelName(log_level))

    run_job()
