import yaml
from pathlib import Path
from sqlalchemy import create_engine
from functools import lru_cache

CONFIG_PATH = Path('config/db.yaml')

class DBConnection:
    def __init__(self):
        self.db_config = self.load_db_config()
        self.engine = self.create_db_engine()

    def load_db_config(self):
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        return config['warehouse']
    def create_db_engine(self):
        username = self.db_config['username']
        password = self.db_config['password']
        host = self.db_config['host']
        port = self.db_config['port']
        database = self.db_config['database']
        return create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")

