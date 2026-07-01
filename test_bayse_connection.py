# test_bayse_connection.py
import os
from dotenv import load_dotenv
load_dotenv()

PUBLIC_KEY = os.getenv("BAYSE_PUBLIC_KEY")
SECRET_KEY = os.getenv("BAYSE_SECRET_KEY")
BASE_URL = "https://relay.bayse.markets"

print(f"Public Key: {PUBLIC_KEY[:20]}..." if PUBLIC_KEY else "No key found")
print(f"Base URL: {BASE_URL}")

# Simple test request
import requests
response = requests.get(f"{BASE_URL}/v1/pm/events?size=10")
print(f"Status: {response.status_code}")
if response.ok:
    data = response.json()
    for event in data.get("events", []):
        print(f"Event: {event.get('title')}")
