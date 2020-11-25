from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from lib.config import config, REGION
from lib.secrets_manager import get_secret

DB_SECRET_ARN = config.get('DB_SECRET_ARN')
DB_CONFIG = get_secret(DB_SECRET_ARN)
PASSWORD = DB_CONFIG['password']
NAME = DB_CONFIG['dbname']
PORT = DB_CONFIG['port']
HOST = DB_CONFIG['host']
USER = DB_CONFIG['username']

engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}')
Session = sessionmaker(bind=engine)
