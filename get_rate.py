import requests

# Free public FX API - no key needed
r = requests.get("https://api.frankfurter.app/latest?from=EUR&to=GBP")
data = r.json()
rate = data["rates"]["GBP"]
print(f"Current EUR/GBP rate: {rate}")
print(f"Market question level: 0.87256")
print(f"Rate vs level: {'ABOVE' if rate > 0.87256 else 'BELOW'}")

# This tells us the probability of YES resolving
# If rate is well below 0.87256, YES is unlikely → low probability
# If rate is just below, it could go either way → ~50%
diff = rate - 0.87256
print(f"Difference: {diff:+.5f}")
