import requests, os
from dotenv import load_dotenv

load_dotenv()
BASE       = os.getenv("BASE_URL")
PUBLIC_KEY = os.getenv("PUBLIC_KEY")
headers    = {"X-Public-Key": PUBLIC_KEY}

# Check wallet
print("=== WALLET ===")
r = requests.get(f"{BASE}/v1/wallet/assets", headers=headers)
print("Status:", r.status_code)
import json
print(json.dumps(r.json(), indent=2))

# Check portfolio
print("\n=== PORTFOLIO ===")
r2 = requests.get(f"{BASE}/v1/pm/portfolio", headers=headers)
print("Status:", r2.status_code)
print(json.dumps(r2.json(), indent=2))
