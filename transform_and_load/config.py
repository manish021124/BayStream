import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
SSL_DISABLED = os.getenv("SSL_DISABLED", "True") == "True"

RAW_TABLE = 'ebay_raw_items'
CLEAN_TABLE = 'ebay_clean_items'
ETL_TRACKER_TABLE = 'etl_tracker'
SELLER_TABLE = 'dim_seller'
ITEMS_TABLE = 'dim_item'
ITEM_LOCATION_TABLE = 'dim_location'
CONDITIONS_TABLE = 'dim_condition'
DATE_TABLE = 'dim_date'
FACT_TABLE = 'fact_listing'
