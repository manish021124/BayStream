import base64
import requests
import logging
from config import CLIENT_ID, CLIENT_SECRET

def get_access_token():
  logging.info("Getting eBay access token...")
  credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
  encoded = base64.b64encode(credentials.encode()).decode()

  headers = {
    "Authorization": f"Basic {encoded}",
    "Content-Type": "application/x-www-form-urlencoded"
  }

  data = {
    "grant_type": "client_credentials",
    "scope": "https://api.ebay.com/oauth/api_scope"
  }

  response = requests.post("https://api.ebay.com/identity/v1/oauth2/token", headers=headers, data=data)
  if response.status_code != 200:
    logging.error(f"Token fetch failed: {response.status_code} - {response.text}")
    return None

  return response.json().get("access_token")
