import pandas as pd
from config import RAW_TABLE, CLEAN_TABLE

def load_raw_data(cursor, last_ts):
  if last_ts:
    query = f"SELECT * FROM {RAW_TABLE} WHERE pull_timestamp > %s"
    cursor.execute(query, (last_ts,))
  else:
    query = f"SELECT * FROM {RAW_TABLE}"
    cursor.execute(query)

  rows = cursor.fetchall()
  return pd.DataFrame(rows)