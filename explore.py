import requests, os, json
from dotenv import load_dotenv

load_dotenv()

BASE       = os.getenv("BASE_URL")
PUBLIC_KEY = os.getenv("PUBLIC_KEY")
headers    = {"X-Public-Key": PUBLIC_KEY}

r = requests.get(f"{BASE}/v1/pm/events", headers=headers)
data   = r.json()

# data is a dict — find the actual list inside it
if isinstance(data, dict):
    # try common wrapper keys
    events = data.get("data") or data.get("events") or data.get("results") or []
else:
    events = data

print(f"Total events found: {len(events)}\n")

for event in events:
    title = (event.get("title") or event.get("name") or "").upper()
    if "EUR" in title or "GBP" in title or "FX" in title or "FOREX" in title:
        print("=== MATCH FOUND ===")
        print("Event ID   :", event.get("id"))
        print("Event Title:", event.get("title") or event.get("name"))
        print("Slug       :", event.get("slug"))
        markets = event.get("markets", [])
        print(f"Markets inside ({len(markets)}):")
        for m in markets:
            print("  Market ID     :", m.get("id"))
            print("  Market Title  :", m.get("title"))
            print("  Status        :", m.get("status"))
            print("  Yes price     :", m.get("outcome1Price"))
            print("  No  price     :", m.get("outcome2Price"))
            print("  Yes outcome ID:", m.get("outcome1Id"))
            print("  No  outcome ID:", m.get("outcome2Id"))
            print()
