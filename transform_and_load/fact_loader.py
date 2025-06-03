import logging
from config import FACT_TABLE

def insert_fact_listing(cursor, fact_rows):
  if not fact_rows:
    logging.info("No fact rows to insert.")
    return 0
  
  insert_query = f'''
    INSERT INTO {FACT_TABLE} (
      item_id, price, condition_id, shipping_cost, shipping_type,
      top_rated, priority_listing, available_coupons, adult_only,
      origin_date_id, created_date_id, seller_id, category_id, location_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
  '''
  params = [
    (
      r['item_id'], r['price'], r['condition_id'], r['shipping_cost'], r['shipping_type'],
      r['is_top_rated'], r['is_priority_listing'], r['available_coupons'], r['adult_only'],
      r['origin_date_id'], r['created_date_id'], r['seller_id'], r['category_id'], r['location_id']
    ) for r in fact_rows
  ]
  try:
    cursor.executemany(insert_query, params)
    logging.info(f"Inserted {cursor.rowcount} fact_listing records.")
    return cursor.rowcount
  except Exception as e:
    logging.error(f"Error inserting fact_listing table: {e}")
    raise
