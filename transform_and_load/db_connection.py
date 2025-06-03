import mysql.connector
from mysql.connector import Error
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, SSL_DISABLED

def get_connection():
  try:
    return mysql.connector.connect(
      host=DB_HOST,
      user=DB_USER,
      password=DB_PASSWORD,
      database=DB_NAME,
      ssl_disabled=SSL_DISABLED
    )
  except Error as e:
    raise Exception(f"Database connection failed: {e}")