import logging
import pandas as pd
from transform_and_load.clean_data import clean_int, clean_string, clean_float, clean_decimal, parse_iso_datetime, is_valid_url

DEFAULT_DATE = '1970-01-01 00:00:00'

def clean_row(df, now_ts):
  def load_and_clean(row):
    try:
      item_id = clean_string(row.get('item_id'), 100)
      if not item_id:
        return None

      condition_id = clean_string(row.get('condition_id')) or '1111'
      item_condition = clean_string(row.get('item_condition'), 100) or 'Unspecified'
      if condition_id == '1000':
        item_condition = 'New'

      title = clean_string(row.get('title')) or 'No Title'
      price = clean_decimal(row.get('price')) or 0.0
      seller_username = clean_string(row.get('seller_username'), 100) or 'Unknown Seller'
      feedback_score = clean_int(row.get('feedback_score')) or 0
      feedback_percentage = clean_float(row.get('feedback_percentage'), default=0.0, max_value=100.0)
      shipping_cost = clean_decimal(row.get('shipping_cost')) or 0.0
      shipping_type = clean_string(row.get('shipping_type'), 50) or 'UNSPECIFIED'

      image = row.get('image')
      image = image.strip() if isinstance(image, str) else ''
      image = image if is_valid_url(image) else ''

      thumbnail = row.get('thumbnail')
      thumbnail = thumbnail.strip() if isinstance(thumbnail, str) else ''
      thumbnail = thumbnail if is_valid_url(thumbnail) else ''

      item_url = row.get('item_url')
      item_url = item_url.strip() if isinstance(item_url, str) else ''
      item_url = item_url if is_valid_url(item_url) else ''

      country = clean_string(row.get('country'), 10) or 'UNKNOWN'
      postal_code = clean_string(row.get('postal_code'), 20) or '00000'
      category_id = clean_string(row.get('category_id'), 20) or '0000'
      is_top_rated = bool(row.get('is_top_rated')) if row.get('is_top_rated') is not None else False
      is_priority_listing = bool(row.get('is_priority_listing')) if row.get('is_priority_listing') is not None else False
      adult_only = bool(row.get('adult_only')) if row.get('adult_only') is not None else False
      available_coupons = bool(row.get('available_coupons')) if row.get('available_coupons') is not None else False

      created_date_raw = row.get('created_date')
      created_date = parse_iso_datetime(created_date_raw)
      if not created_date:
        created_date = DEFAULT_DATE

      origin_date_raw = row.get('origin_date')
      origin_date = parse_iso_datetime(origin_date_raw)
      if not origin_date:
        origin_date = DEFAULT_DATE

      return {
        'item_id': item_id,
        'title': title,
        'price': price,
        'item_condition': item_condition,
        'condition_id': condition_id,
        'seller_username': seller_username,
        'feedback_score': feedback_score,
        'feedback_percentage': feedback_percentage,
        'shipping_cost': shipping_cost,
        'shipping_type': shipping_type,
        'image': image,
        'thumbnail': thumbnail,
        'item_url': item_url,
        'country': country,
        'postal_code': postal_code,
        'category_id': category_id,
        'is_top_rated': is_top_rated,
        'is_priority_listing': is_priority_listing,
        'adult_only': adult_only,
        'available_coupons': available_coupons,
        'created_date': created_date,
        'origin_date': origin_date,
        'etl_timestamp': now_ts
      }
    except Exception as e:
      logging.warning(f"Skipping record {row['id']} due to error: {e}")
      return None
              
  cleaned_rows = df.apply(load_and_clean, axis=1)
  cleaned_df = pd.DataFrame([row for row in cleaned_rows if row and row['item_id'] and row['created_date'] and row['origin_date']])

  cleaned_df.sort_values(by='etl_timestamp', ascending=False, inplace=True)
  cleaned_df.drop_duplicates(subset='item_id', keep='first', inplace=True)
  return cleaned_df