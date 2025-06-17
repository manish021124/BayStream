import re
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation

URL_PATTERN = re.compile(r'^(http|https)://')

def parse_iso_datetime(value, return_date=False):
  for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
    try:
      dt = datetime.strptime(value, fmt)
      return dt.date() if return_date else dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
      continue
  return None
  
def clean_int(value, default=0, min_value=None, max_value=None):
  try:
    i = int(value)
    if min_value is not None:
      i = max(i, min_value)
    if max_value is not None:
      i = min(i, max_value)
    return i
  except (ValueError, TypeError):
    return default
    
def clean_string(s, max_length=None):
  if s is None:
    return None
  if not isinstance(s, str):
    s = str(s)
  s = unicodedata.normalize('NFKC', s.strip())
  s = re.sub(r'\s+', ' ', s)
  return s[:max_length] if max_length else s

def clean_float(val, default=0.0, max_value=None):
  try:
    f = float(val)
    if max_value is not None:
      return min(f, max_value)
    return f
  except (TypeError, ValueError):
    return default

def clean_decimal(val, default=Decimal('0.00')):
  try:
    return Decimal(val).quantize(Decimal('0.01'))
  except (InvalidOperation, TypeError, ValueError):
    return default
  
def is_valid_url(url):
  if not url or not isinstance(url, str):
    return False
  url = url.strip()
  return bool(URL_PATTERN.match(url))
