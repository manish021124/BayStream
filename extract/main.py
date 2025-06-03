import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import requests
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from auth import get_access_token
from scraper import search_items_by_category
from config import THREADS
from db import setup_mysql, get_random_category_ids, pool
from logger import ETLLogger

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_category(category_id, session, pull_ts):
  try:
    inserted = search_items_by_category(session, category_id, pull_ts)
    return inserted
  except Exception as e:
    logging.error(f"Category '{category_id}' failed: {e}")
    return 0

def main():
  setup_mysql()

  token = get_access_token()
  if not token:
    logging.error("Token generation failed. Exiting.")
    return
  
  pull_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

  session = requests.Session()
  session.headers.update({
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
  })

  logger = ETLLogger(pool.get_connection, job_name="fetch_api")

  try:
    logger.start("get random categories for fetching api")
    category_ids = get_random_category_ids()
    if not category_ids:
      raise ValueError("No categories available.")
    logger.success(records_processed=len(category_ids))
    logging.info(f"Selected {len(category_ids)} random categories")
  except Exception:
    logger.fail(traceback.format_exc())
    logging.error("Failed to retrieve category IDs.")
    return
  
  total_inserted = 0
  try:
    logger.start("insert raw data to db")
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
      futures = {
        executor.submit(process_category, category_id, session, pull_ts): category_id 
        for category_id in category_ids
      }
      for future in as_completed(futures):
        category_id = futures[future]
        try:
          inserted = future.result()
          total_inserted += inserted
        except Exception as exc:
          logging.error(f"Error processing category '{category_id}': {exc}")
    logger.success(records_processed=total_inserted)
  except:
    logger.fail(traceback.format_exc())
    raise

if __name__ == "__main__":
  main()
