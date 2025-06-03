import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
SSL_DISABLED = os.getenv("SSL_DISABLED", "True") == "True"

TABLE_NAME = "ebay_raw_items"
TRACKER_TABLE = "ebay_offset_tracker"
THREADS = int(os.getenv("THREADS", 5))
MAX_ITEMS_PER_TERM = 1000
MAX_RETRIES = 3
BACKOFF_FACTOR = 5
