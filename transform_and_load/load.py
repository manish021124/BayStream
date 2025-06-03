import logging
from datetime import datetime
from dim_loaders import (
  insert_dim_condition, insert_dim_date, insert_dim_item, insert_dim_location, 
  insert_dim_seller, get_seller_lookup, get_location_lookup, get_condition_lookup
)
from fact_loader import insert_fact_listing
from config import CLEAN_TABLE

def parse_date(d):
  if isinstance(d, datetime):
    return d.strftime('%Y%m%d')
  if isinstance(d, str):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
      try:
        return datetime.strptime(d, fmt).strftime('%Y%m%d')
      except ValueError:
        continue
    return None

def bulk_insert_clean_data(cursor, clean_df):
  insert_query = f'''
    INSERT INTO {CLEAN_TABLE} (
      item_id, title, price, item_condition, condition_id, seller_username,
      feedback_score, feedback_percentage, shipping_cost, shipping_type, image, thumbnail,
      item_url, country, postal_code, category_id, is_top_rated,
      is_priority_listing, adult_only, available_coupons,
      created_date, origin_date, etl_timestamp
    ) VALUES (
      %(item_id)s, %(title)s, %(price)s, %(item_condition)s, %(condition_id)s,
      %(seller_username)s, %(feedback_score)s, %(feedback_percentage)s, %(shipping_cost)s,
      %(shipping_type)s, %(image)s, %(thumbnail)s, %(item_url)s, %(country)s, %(postal_code)s,
      %(category_id)s, %(is_top_rated)s, %(is_priority_listing)s, %(adult_only)s,
      %(available_coupons)s, %(created_date)s, %(origin_date)s, %(etl_timestamp)s
    )
    ON DUPLICATE KEY UPDATE
      title = VALUES(title),
      price = VALUES(price),
      item_condition = VALUES(item_condition),
      condition_id = VALUES(condition_id),
      seller_username = VALUES(seller_username),
      feedback_score = VALUES(feedback_score),
      feedback_percentage = VALUES(feedback_percentage),
      shipping_cost = VALUES(shipping_cost),
      shipping_type = VALUES(shipping_type),
      image = VALUES(image),
      thumbnail = VALUES(thumbnail),
      item_url = VALUES(item_url),
      country = VALUES(country),
      postal_code = VALUES(postal_code),
      category_id = VALUES(category_id),
      is_top_rated = VALUES(is_top_rated),
      is_priority_listing = VALUES(is_priority_listing),
      adult_only = VALUES(adult_only),
      available_coupons = VALUES(available_coupons),
      created_date = VALUES(created_date),
      origin_date = VALUES(origin_date),
      etl_timestamp = VALUES(etl_timestamp)
  '''
  clean_rows = clean_df.to_dict('records')
  cursor.executemany(insert_query, clean_rows)
  logging.info(f"Inserted/Updated {cursor.rowcount} records.")
  return cursor.rowcount

def load_dimensions_and_facts(cursor, clean_df):
  if clean_df.empty:
    logging.info("No rows to load into dimensions and facts.")
    return
    
  conditions = set(clean_df[['condition_id', 'item_condition']].itertuples(index=False, name=None))
  sellers = clean_df.drop_duplicates(subset=['seller_username']).to_dict('records')
  items = clean_df.drop_duplicates(subset=['item_id']).to_dict('records')
  locations = set(clean_df[['country', 'postal_code']].itertuples(index=False, name=None))
    
  unique_dates = set(clean_df['origin_date'].dropna().astype(str).tolist() + 
    clean_df['created_date'].dropna().astype(str).tolist())

  insert_dim_date(cursor, unique_dates)
  insert_dim_seller(cursor, sellers)
  insert_dim_item(cursor, items)
  insert_dim_condition(cursor, conditions)
  insert_dim_location(cursor, locations)

  condition_lookup = get_condition_lookup(cursor)
  seller_lookup = get_seller_lookup(cursor)
  location_lookup = get_location_lookup(cursor)

  fact_rows = []
  for _, r in clean_df.iterrows():
    try:
      origin_date_id = parse_date(r['origin_date'])
      created_date_id = parse_date(r['created_date'])

      fact_rows.append({
        'item_id': r['item_id'],
        'price': r['price'],
        'condition_id': condition_lookup[r['condition_id']],
        'shipping_cost': r['shipping_cost'],
        'shipping_type': r['shipping_type'],
        'is_top_rated': r['is_top_rated'],
        'is_priority_listing': r['is_priority_listing'],
        'available_coupons': r['available_coupons'],
        'adult_only': r['adult_only'],
        'origin_date_id': origin_date_id,
        'created_date_id': created_date_id,
        'seller_id': seller_lookup[r['seller_username']],
        'category_id': r['category_id'],
        'location_id': location_lookup[(r['country'], r['postal_code'])],
      })
    except KeyError as e:
      logging.warning(f"Missing FK: {e} for item_id={r['item_id']}")
    except Exception as e:
      logging.error(f"Error processing fact row for item_id={r.get('item_id')}: {e}")

  inserted = insert_fact_listing(cursor, fact_rows)
  logging.info(f"Inserted records in dimension and fact tables.")
  return inserted