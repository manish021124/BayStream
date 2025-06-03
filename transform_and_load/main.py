import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import traceback
from datetime import datetime
from schema_setup import setup_etl_tables
from transform_and_load.db_connection import get_connection
from etl_tracker import get_last_etl_timestamp, update_etl_timestamp
from transform import clean_row
from load import load_dimensions_and_facts, bulk_insert_clean_data
from transform_and_load.load_from_db import load_raw_data, load_clean_data
from logger import ETLLogger

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')  

def main():
  setup_etl_tables()

  last_ts = get_last_etl_timestamp()
  now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

  conn = get_connection()
  cursor = conn.cursor(dictionary=True)
  logger = ETLLogger(get_connection, job_name="ebay_etl_pipeline")

  try:
    try:
      logger.start("load raw data from db")
      raw_df = load_raw_data(cursor, last_ts)
      logging.info(f"Fetched {len(raw_df)} raw records.")

      if raw_df.empty:
        logging.info("No new raw records to process.")
        logger.success(records_processed=0)
        return
      logger.success(records_processed=len(raw_df))
    except Exception:
      logger.fail(traceback.format_exc())
      raise
    
    try:
      logger.start("clean records")
      clean_df = clean_row(raw_df, now_ts)

      if clean_df.empty:
        logging.info("No valid rows after cleaning.")
        logger.success(records_processed=0)
        return
      logger.success(records_processed=len(clean_df))
    except Exception:
      logger.fail(traceback.format_exc())
      raise
    
    try:
      logger.start("insert clean data to db")
      clean_data_inserted = bulk_insert_clean_data(cursor, clean_df)
      conn.commit()
      logger.success(records_processed=clean_data_inserted)
    except Exception:
      logger.fail(traceback.format_exc())
      raise

    try:
      logger.start("load clean data from db")
      clean_db_df = load_clean_data(cursor, last_ts)
      logger.success(records_processed=len(clean_db_df))
    except Exception:
      logger.fail(traceback.format_exc())
      raise

    try:
      logger.start("insert data to dimension and fact tables")
      inserted = load_dimensions_and_facts(cursor, clean_db_df)
      conn.commit()
      logger.success(records_processed=inserted)
    except Exception:
      logger.fail(traceback.format_exc())
      raise

    update_etl_timestamp(now_ts)
    logging.info("ETL pipeline completed successfully.")
  except Exception as e:
    logging.error(f"ETL pipeline failed: {e}", exc_info=True)
  finally:
    cursor.close()
    conn.close()

if __name__ == "__main__":
  main()