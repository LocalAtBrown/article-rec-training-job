from peewee import PostgresqlDatabase

from lib.config import config

PASSWORD = config.get("DB_PASSWORD")
NAME = config.get("DB_NAME")
USER = config.get("DB_USER")
HOST = config.get("DB_HOST")
PORT = 5432  # default postgres port

db = PostgresqlDatabase(NAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
