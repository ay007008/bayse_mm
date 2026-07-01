import requests, os, json
from dotenv import load_dotenv

load_dotenv()
BASE       = os.getenv("BASE_URL")
PUBLIC_KEY = os.getenv("PUBLIC_KEY")
headers    = {"X-Public-Key": PUBLIC_KEY}

# Find current EURGBP market
r = requests.get(f"{BASE}/v1/pm/events", headers=headers)
data   = r.json()
events = data.get("data") or data.get("events") or data.get("results") or []

if isinstance(events, dict):
    events = list(events.values())[0]

print(f"Total events: {len(events)}\n")
for event in events:
    title = (event.get("title") or event.get("name") or "").upper()
    if "EUR" in title or "GBP" in title:
        print("=== EURGBP EVENT ===")
        print("Event ID   :", event.get("id"))
        print("Event Title:", event.get("title") or event.get("name"))
        markets = event.get("markets", [])
        for m in markets:
            print("  Market ID     :", m.get("id"))
            print("  Market Title  :", m.get("title"))
            print("  Status        :", m.get("status"))
            print("  outcome1Id    :", m.get("outcome1Id"))
            print("  outcome2Id    :", m.get("outcome2Id"))
            print("  outcome1Price :", m.get("outcome1Price"))
            print("  outcome2Price :", m.get("outcome2Price"))
            print()
