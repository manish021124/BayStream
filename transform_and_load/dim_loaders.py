from datetime import datetime
import calendar
import logging
from config import SELLER_TABLE, ITEMS_TABLE, ITEM_LOCATION_TABLE, CONDITIONS_TABLE, DATE_TABLE

def insert_dim_seller(cursor, sellers):
  for s in sellers:
    try:
      cursor.execute(f'''
        INSERT INTO {SELLER_TABLE} (
          username, feedback_percentage, feedback_score
        ) VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          feedback_percentage = VALUES(feedback_percentage),
          feedback_score = VALUES(feedback_score)
      ''', (
        s['seller_username'], s['feedback_percentage'], s['feedback_score']
      ))
    except Exception as e:
      logging.error(f"Error inserting/updating seller {s['seller_username']}: {e}")

def insert_dim_item(cursor, items):
  for item in items:
    try:
      cursor.execute(f'''
        INSERT INTO {ITEMS_TABLE} (
          item_id, title, item_url, image, thumbnail
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          title = VALUES(title),
          item_url = VALUES(item_url),
          image = VALUES(image),
          thumbnail = VALUES(thumbnail)
      ''', (
        item['item_id'], item['title'], item['item_url'],
        item['image'], item['thumbnail']
      ))
    except Exception as e:
      logging.error(f"Error inserting item {item['item_id']}: {e}")

def insert_dim_location(cursor, locations):  
  try:
    cursor.execute(f"SELECT country, postal_code FROM {ITEM_LOCATION_TABLE}")
    existing = set((row['country'], row['postal_code']) for row in cursor.fetchall())

    seen = set()

    for country, postal_code in locations:
      key = (country, postal_code)

      if key in existing or key in seen:
        continue

      cursor.execute(f'''
        INSERT INTO {ITEM_LOCATION_TABLE} (country, postal_code)
        VALUES (%s, %s)
      ''', (
        country, postal_code
      ))

      seen.add(key)
  except Exception as e:
    logging.error(f"Error inserting location ({country}, {postal_code}): {e}")

def insert_dim_condition(cursor, conditions):
  for condition_id, condition_name in conditions:
    try:
      cursor.execute(f'''
        INSERT INTO {CONDITIONS_TABLE} (condition_id, condition_name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE condition_name = VALUES(condition_name)
      ''', (condition_id, condition_name))
    except Exception as e:
      logging.error(f"Error inserting condition ({condition_id}, {condition_name}): {e}")


def insert_dim_date(cursor, date_strings):
  seen = set()
  for date_str in date_strings:
    try:
      date_only = date_str.split(" ")[0]
      if date_only in seen:
        continue
      seen.add(date_only)

      parsed_date = datetime.strptime(date_only, "%Y-%m-%d")

      date_id = parsed_date.strftime("%Y%m%d")
      cursor.execute(f"SELECT 1 FROM {DATE_TABLE} WHERE date_id = %s LIMIT 1", (date_id,))
      if cursor.fetchone():
        continue

      year = parsed_date.year
      month = parsed_date.month
      day = parsed_date.day
      week = parsed_date.isocalendar()[1]
      quarter = (month - 1) // 3 + 1
      day_of_week = parsed_date.weekday() + 1
      is_weekend = 1 if day_of_week >= 6 else 0
      month_name = calendar.month_name[month]
      day_name = calendar.day_name[parsed_date.weekday()]

      cursor.execute(f'''
        INSERT INTO {DATE_TABLE} (
          date_id, year, month, day, week, quarter, 
          day_of_week, is_weekend, month_name, day_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
          date_id, year, month, day, week, quarter,
          day_of_week, is_weekend, month_name, day_name
        ))
    except Exception as e:
      logging.error(f"Error inserting date {date_str}: {e}")

def get_condition_lookup(cursor):
  cursor.execute(f"SELECT condition_id FROM {CONDITIONS_TABLE}")
  rows = cursor.fetchall()
  return { row['condition_id']: row['condition_id'] for row in rows }

def get_seller_lookup(cursor):
  cursor.execute(f"SELECT seller_id, username FROM {SELLER_TABLE}")
  rows = cursor.fetchall()
  return { row['username']: row['seller_id'] for row in rows }

def get_location_lookup(cursor):
  cursor.execute(f"SELECT location_id, country, postal_code FROM {ITEM_LOCATION_TABLE}")
  rows = cursor.fetchall()
  return { (row['country'], row['postal_code']): row['location_id'] for row in rows }
