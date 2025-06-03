import time
import random
import logging
from config import MAX_RETRIES, BACKOFF_FACTOR, MAX_ITEMS_PER_TERM
from db import get_last_offset, update_last_offset, insert_items

def search_items_by_category(session, category_id, pull_ts):
  logging.info(f"Started category '{category_id}'")
  offset = get_last_offset(str(category_id))
  start_offset = offset
  all_items = []
  retry_count = 0

  while True:
    url = f"https://api.ebay.com/buy/browse/v1/item_summary/search?category_ids={category_id}&limit=100&offset={offset}"
    response = session.get(url)

    if response.status_code in [429, 502, 503, 504]:
      wait = BACKOFF_FACTOR * (2 ** retry_count) + random.uniform(0.5, 1.5)
      logging.warning(f"Rate limit or error {response.status_code}. Retrying in {wait:.1f}s...")
      time.sleep(wait)
      retry_count += 1
      if retry_count >= MAX_RETRIES:
        logging.error(f"Too many retries for '{category_id}' at offset {offset}")
        break
      continue

    if response.status_code != 200:
      logging.error(f"Failed {response.status_code}: {response.text}")
      break

    retry_count = 0
    data = response.json()
    items = data.get("itemSummaries", [])
    if not items:
      break

    for item in items:
      try:
        all_items.append({
          'item_id': item.get('itemId'),
          'title': item.get('title'),
          'price': item.get('price', {}).get('value'),
          'item_condition': item.get('condition'),
          'condition_id': item.get('conditionId'),
          'seller_username': item.get('seller', {}).get('username'),
          'feedback_score': item.get('seller', {}).get('feedbackScore'),
          'feedback_percentage': item.get('seller', {}).get('feedbackPercentage'),
          'shipping_cost': item.get('shippingOptions', [{}])[0].get('shippingCost', {}).get('value'),
          'shipping_type': item.get('shippingOptions', [{}])[0].get('shippingCostType'),
          'image': item.get('image', {}).get('imageUrl'),
          'thumbnail': item.get('thumbnailImages', [{}])[0].get('imageUrl'),
          'item_url': item.get('itemWebUrl'),
          'country': item.get('itemLocation', {}).get('country'),
          'postal_code': item.get('itemLocation', {}).get('postalCode'),
          'category_id': category_id,
          'is_top_rated': item.get('topRatedListing'),
          'is_priority_listing': item.get('priorityListing'),
          'adult_only': item.get('adultOnly'),
          'available_coupons': item.get('availableCoupons'),
          'created_date': item.get('itemCreationDate'),
          'origin_date': item.get('itemOriginDate')
        })
      except Exception as e:
        logging.warning(f"Item parse error: {e}")

    offset += 100
    update_last_offset(str(category_id), offset)

    total = data.get("total", 0)
    if offset - start_offset >= MAX_ITEMS_PER_TERM or offset >= total:
      logging.info(f"Paused category '{category_id}' at offset {offset}")
      break

    time.sleep(1.5)

  try:    
    insert_items(str(category_id), all_items, pull_ts)
  except Exception:
    return 0
  
  return len(all_items)
