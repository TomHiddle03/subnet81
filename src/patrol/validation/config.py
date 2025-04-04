import os
from sqlalchemy.ext.asyncio import create_async_engine

NETWORK = os.getenv('NETWORK', "finney")
NET_UID = int(os.getenv('NET_UID', "81"))

DB_DIR = os.getenv('DB_DIR', "/tmp/sqlite")
DB_URL = os.getenv("DB_URL", f"sqlite+aiosqlite:///{DB_DIR}/patrol.db")

db_engine = create_async_engine(DB_URL)

WALLET_NAME = os.environ['WALLET_NAME']
HOTKEY_NAME = os.environ['HOTKEY_NAME']
BITTENSOR_PATH = os.getenv('BITTENSOR_PATH')