from mysql.connector import pooling
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, SSL_DISABLED, TABLE_NAME, TRACKER_TABLE
import logging

pool = pooling.MySQLConnectionPool(
  pool_name="ebay_extract_pool",
  pool_size=10,
  host=DB_HOST,
  user=DB_USER,
  password=DB_PASSWORD,
  database=DB_NAME,
  ssl_disabled=SSL_DISABLED
)

def setup_mysql():
  conn = pool.get_connection()
  cursor = conn.cursor()
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
      id INT AUTO_INCREMENT PRIMARY KEY,
      item_id VARCHAR(100),
      title TEXT,
      price DECIMAL(10,2),
      item_condition VARCHAR(100),
      condition_id VARCHAR(20),
      seller_username TEXT,
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
      created_date VARCHAR(50),
      origin_date VARCHAR(50),
      pull_timestamp DATETIME
    )
  ''')
  cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {TRACKER_TABLE} (
      category_id VARCHAR(50) PRIMARY KEY,
      last_offset INT DEFAULT 0
    )
  ''')
  conn.commit()
  cursor.close()
  conn.close()

def get_last_offset(category_id):
  conn = pool.get_connection()
  cursor = conn.cursor()
  cursor.execute(f"SELECT last_offset FROM {TRACKER_TABLE} WHERE category_id = %s", (category_id,))
  result = cursor.fetchone()
  cursor.close()
  conn.close()
  return result[0] if result else 0

def update_last_offset(category_id, offset):
  conn = pool.get_connection()
  cursor = conn.cursor()
  cursor.execute(f'''
    INSERT INTO {TRACKER_TABLE} (category_id, last_offset)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE last_offset = VALUES(last_offset)
  ''', (category_id, offset))
  conn.commit()
  cursor.close()
  conn.close()

def insert_items(category_id, items, pull_ts):
  if not items:
    return
  conn = pool.get_connection()
  cursor = conn.cursor()
  try:
    query = f'''
      INSERT INTO {TABLE_NAME} (
        item_id, title, price, item_condition, condition_id, seller_username,
        feedback_score, feedback_percentage, shipping_cost, shipping_type,
        image, thumbnail, item_url, country, postal_code, category_id,
        is_top_rated, is_priority_listing, adult_only, available_coupons,
        created_date, origin_date, pull_timestamp
      ) VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s
      )
    '''

    values = []
    for item in items:
      values.append((
        item.get('item_id'),
        item.get('title'),
        item.get('price'),
        item.get('item_condition'),
        item.get('condition_id'),
        item.get('seller_username'),
        item.get('feedback_score'),
        item.get('feedback_percentage'),
        item.get('shipping_cost'),
        item.get('shipping_type'),
        item.get('image'),
        item.get('thumbnail'),
        item.get('item_url'),
        item.get('country'),
        item.get('postal_code'),
        category_id,
        item.get('is_top_rated'),
        item.get('is_priority_listing'),
        item.get('adult_only'),
        item.get('available_coupons'),
        item.get('created_date'),
        item.get('origin_date'),
        pull_ts
      ))

    cursor.executemany(query, values)
    conn.commit()
    logging.info(f"Inserted {cursor.rowcount} items for category '{category_id}'")
  except Exception as e:
    logging.error(f"Insert failed for category '{category_id}': {e}")
    conn.rollback()
  finally:
    cursor.close()
    conn.close()

def get_random_category_ids(limit=90):
  conn = pool.get_connection()
  cursor = conn.cursor()
  try:
    cursor.execute("SELECT category_id FROM ebay_categories ORDER BY RAND() LIMIT %s", (limit,))
    rows = cursor.fetchall()
    return [row[0] for row in rows]
  except Exception as e:
    logging.error(f"Failed to fetch random category IDs: {e}")
    return []
  finally:
    cursor.close()
    conn.close()
