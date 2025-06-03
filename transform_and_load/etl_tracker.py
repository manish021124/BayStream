from transform_and_load.db_connection import get_connection
from config import ETL_TRACKER_TABLE

def get_last_etl_timestamp():
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute(f"SELECT last_etl_pull FROM {ETL_TRACKER_TABLE} WHERE id = 1")
  row = cursor.fetchone()
  cursor.close()
  conn.close()
  return row[0] if row else None

def update_etl_timestamp(ts):
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute(f'''
    INSERT INTO {ETL_TRACKER_TABLE} (id, last_etl_pull)
    VALUES (1, %s)
    ON DUPLICATE KEY UPDATE last_etl_pull = VALUES(last_etl_pull)
  ''', (ts,))
  conn.commit()
  cursor.close()
  conn.close()