from transform_and_load.db_connection import get_connection
from config import ETL_TRACKER_TABLE, CLEAN_TABLE, SELLER_TABLE, ITEMS_TABLE, ITEM_LOCATION_TABLE, CONDITIONS_TABLE, DATE_TABLE, FACT_TABLE

def setup_etl_tables():
  conn = get_connection()
  cursor = conn.cursor()

  # etl tracker
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {ETL_TRACKER_TABLE} (
      id INT PRIMARY KEY DEFAULT 1,
      last_etl_pull DATETIME
    )
  ''')

  # complete clean table
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {CLEAN_TABLE} (
      item_id VARCHAR(100) PRIMARY KEY,
      title TEXT,
      price DECIMAL(10,2),
      item_condition VARCHAR(100),
      condition_id VARCHAR(20),
      seller_username VARCHAR(100),
      feedback_score INT,
      feedback_percentage FLOAT,
      shipping_cost DECIMAL(10,2),
      shipping_type VARCHAR(50),
      image TEXT,
      thumbnail TEXT,
      item_url TEXT,
      country VARCHAR(10),
      postal_code VARCHAR(20),
      category_id VARCHAR(20),
      is_top_rated BOOLEAN,
      is_priority_listing BOOLEAN,
      adult_only BOOLEAN,
      available_coupons BOOLEAN,
      created_date DATETIME,
      origin_date DATETIME,
      etl_timestamp DATETIME
    )
  ''')

  # dim_seller
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {SELLER_TABLE} (
      seller_id INT AUTO_INCREMENT PRIMARY KEY,
      username VARCHAR(100) UNIQUE,
      feedback_score INT,
      feedback_percentage FLOAT
    )
  ''')

  # dim_item
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {ITEMS_TABLE} (
      item_id VARCHAR(100) PRIMARY KEY,
      title TEXT,
      item_url TEXT,
      image TEXT,
      thumbnail TEXT
    )
  ''')

  # dim_location
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {ITEM_LOCATION_TABLE} (
      location_id INT AUTO_INCREMENT PRIMARY KEY,
      country VARCHAR(10),
      postal_code VARCHAR(20)
    )
  ''')

  # dim_condition
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {CONDITIONS_TABLE} (
      condition_id VARCHAR(20) PRIMARY KEY,
      condition_name VARCHAR(100)
    )
  ''')

  # dim_date
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {DATE_TABLE} (
      date_id DATE PRIMARY KEY,
      year INT,
      month INT,
      day INT,
      week INT,
      quarter INT,
      day_of_week INT,
      is_weekend BOOLEAN,
      month_name VARCHAR(20),
      day_name VARCHAR(20)
    )
  ''')

  # fact_listing
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {FACT_TABLE} (
      fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
      item_id VARCHAR(100),
      price DECIMAL(10, 2),
      condition_id VARCHAR(20),
      shipping_cost DECIMAL(10, 2),
      shipping_type VARCHAR(50),
      top_rated BOOLEAN,
      priority_listing BOOLEAN,
      available_coupons BOOLEAN,
      adult_only BOOLEAN,
      origin_date_id DATE,
      created_date_id DATE,
      seller_id INT,
      category_id VARCHAR(20),
      location_id INT,
      FOREIGN KEY (item_id) REFERENCES dim_item(item_id),
      FOREIGN KEY (condition_id) REFERENCES dim_condition(condition_id),
      FOREIGN KEY (origin_date_id) REFERENCES dim_date(date_id),
      FOREIGN KEY (created_date_id) REFERENCES dim_date(date_id),
      FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id),
      FOREIGN KEY (category_id) REFERENCES ebay_categories(category_id),
      FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
    )
  ''')

  conn.commit()
  cursor.close()
  conn.close()