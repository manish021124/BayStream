import requests
import logging
import mysql.connector
from dotenv import load_dotenv
from auth import get_access_token
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, SSL_DISABLED

load_dotenv()
logging.basicConfig(level=logging.INFO)

MARKETPLACE_ID = "EBAY_US"
TAXONOMY_API_BASE = "https://api.ebay.com/commerce/taxonomy/v1"

# MySQL config
MYSQL_CONFIG = {
  'host': DB_HOST,
  'user': DB_USER,
  'password': DB_PASSWORD,
  'database': DB_NAME,
  'ssl_disabled': SSL_DISABLED
}

def fetch_category_tree_id(token, marketplace_id):
  url = f"{TAXONOMY_API_BASE}/get_default_category_tree_id?marketplace_id={marketplace_id}"
  headers = {"Authorization": f"Bearer {token}"}
  response = requests.get(url, headers=headers)
  response.raise_for_status()
  return response.json()["categoryTreeId"]

def fetch_full_category_tree(token, category_tree_id):
  url = f"{TAXONOMY_API_BASE}/category_tree/{category_tree_id}"
  headers = {"Authorization": f"Bearer {token}"}
  response = requests.get(url, headers=headers)
  response.raise_for_status()
  return response.json()

def flatten_categories(categories, parent_id=None, level=0):
  flat = []
  for cat in categories:
    category_node = cat.get("category", {})
    category_id = category_node.get("categoryId")
    category_name = category_node.get("categoryName")

    entry = {
      "category_id": category_id,
      "category_name": category_name,
      "parent_category_id": parent_id,
      "level": level
    }
    flat.append(entry)

    if "childCategoryTreeNodes" in cat and cat["childCategoryTreeNodes"]:
      flat.extend(flatten_categories(cat["childCategoryTreeNodes"], category_id, level + 1))
  return flat

def save_to_mysql(categories):
  conn = mysql.connector.connect(**MYSQL_CONFIG)
  cursor = conn.cursor()
  cursor.execute('''
    CREATE TABLE IF NOT EXISTS ebay_categories (
      category_id VARCHAR(20) PRIMARY KEY,
      category_name VARCHAR(255),
      parent_category_id VARCHAR(20),
      level INT,
      FOREIGN KEY (parent_category_id) REFERENCES ebay_categories(category_id)
    )
  ''')
  cursor.execute('DELETE FROM ebay_categories')

  insert_query = '''
    INSERT INTO ebay_categories (category_id, category_name, parent_category_id, level)
    VALUES (%s, %s, %s, %s)
  '''
  values = [(cat["category_id"], cat["category_name"], cat["parent_category_id"], cat["level"]) for cat in categories]
  cursor.executemany(insert_query, values)
  conn.commit()
  cursor.close()
  conn.close()
  logging.info(f"Inserted {len(categories)} categories into MySQL")

def main():
  try:
    token = get_access_token()
    tree_id = fetch_category_tree_id(token, MARKETPLACE_ID)
    logging.info(f"Using category tree ID: {tree_id}")
        
    full_tree = fetch_full_category_tree(token, tree_id)
    flat_categories = flatten_categories(full_tree["rootCategoryNode"].get("childCategoryTreeNodes", []))
    save_to_mysql(flat_categories)

  except Exception as e:
    logging.error(f"Failed: {e}", exc_info=True)

if __name__ == "__main__":
  main()
