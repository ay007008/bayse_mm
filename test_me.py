import requests, json
r = requests.get("https://relay.bayse.markets/v1/pm/events", 
                 params={"keyword":"SOL","status":"open","page":1,"size":5}, timeout=10)
for e in r.json().get("events",[]):
    if "SOL" in (e.get("title") or "").upper():
        r2 = requests.get(f"https://relay.bayse.markets/v1/pm/events/{e['id']}", timeout=10)
        print(json.dumps(r2.json(), indent=2)[:3000])
        break
