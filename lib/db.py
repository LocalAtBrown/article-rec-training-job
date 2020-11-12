import logging

import psycopg2

from lib.config import config, REGION
from lib.secrets_manager import get_secret


DB_SECRET_KEY = config.get('DB_SECRET_KEY')
DB_CONFIG = get_secret(DB_SECRET_KEY)
PASSWORD = DB_CONFIG['password']
NAME = DB_CONFIG['dbname']
PORT = DB_CONFIG['port']
ENDPOINT = DB_CONFIG['host']
USER = DB_CONFIG['username']


def test_query():
    query_results = ''

    try:
        conn = psycopg2.connect(host=ENDPOINT,
                                port=PORT,
                                database=NAME,
                                user=USER,
                                password=PASSWORD)
        cur = conn.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
    except Exception:
        logging.exception('query failed')

    return query_results
