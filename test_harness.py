"""
Bayse MM v8 — Paper Trading / Dry-Run Test Harness
====================================================

Intercepts all requests.get / requests.post / requests.delete calls and
replaces them with synthetic responses. The bot runs its real logic
(fair value, skew, adverse selection, close-out, P&L) but never touches
the live API.

USAGE
-----
  python test_harness.py                          # run default scenario
  python test_harness.py --scenario closeout      # force close-out path
  python test_harness.py --scenario adverse       # trigger adverse selection
  python test_harness.py --scenario skew          # trigger inventory skew
  python test_harness.py --cycles 8               # run N cycles then stop
  python test_harness.py --speed 0                # no sleep between cycles

SCENARIO CATALOGUE
------------------
  normal    — stable rate, fills both sides roughly evenly
  closeout  — starts with 3 min left, exercises burn + residual liquidation
  adverse   — YES fills 90% of cycles, triggers spread widening
  skew      — mints but NO never fills, builds YES surplus, triggers skew
  nocash    — wallet below MIN_BALANCE_USD, should pause
  capreach  — session_mint_cost near SESSION_MAX_MINT cap
"""

import argparse, json, sys, time, uuid
from unittest.mock import patch, MagicMock

# ── Parse CLI args BEFORE importing the bot ──────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--scenario", default="normal",
                    choices=["normal","closeout","adverse","skew","nocash","capreach"])
parser.add_argument("--cycles", type=int, default=6)
parser.add_argument("--speed",  type=float, default=0.05,
                    help="Sleep multiplier (0=instant, 1=real-time)")
args = parser.parse_args()

SCENARIO = args.scenario
MAX_CYCLES = args.cycles
SPEED_MUL  = args.speed

print(f"\n{'='*60}")
print(f"  PAPER TRADE HARNESS  |  scenario={SCENARIO}  cycles={MAX_CYCLES}")
print(f"{'='*60}\n")

# ── Fake market constants ─────────────────────────────────────────────────────
FAKE_EVENT_ID   = str(uuid.uuid4())
FAKE_MARKET_ID  = str(uuid.uuid4())
FAKE_YES_ID     = str(uuid.uuid4())
FAKE_NO_ID      = str(uuid.uuid4())
FAKE_ORDER_POOL = {}          # order_id -> fake order state
fake_yes_inv    = 0.0         # shares we "own"
fake_no_inv     = 0.0
fake_balance    = 50.0        # starting wallet
fake_cycle      = [0]         # mutable counter

# Scenario-driven rate series (EUR/GBP)
RATE_SERIES = {
    "normal":   [0.8490, 0.8492, 0.8488, 0.8495, 0.8491, 0.8487, 0.8490, 0.8493],
    "closeout": [0.8490] * 10,   # rate doesn't matter — close-out triggered by time
    "adverse":  [0.8470, 0.8468, 0.8466, 0.8465, 0.8464, 0.8463, 0.8462, 0.8461],
    "skew":     [0.8490] * 10,
    "nocash":   [0.8490] * 10,
    "capreach": [0.8490] * 10,
}
RATE_LIST = RATE_SERIES[SCENARIO]

def get_fake_rate():
    idx = min(fake_cycle[0], len(RATE_LIST) - 1)
    return RATE_LIST[idx]

def fake_minutes_left():
    if SCENARIO == "closeout":
        # Start inside close-out window
        return max(0.0, 3.0 - fake_cycle[0] * 0.5)
    return max(0.0, 60.0 - fake_cycle[0] * 8)

# ── Fill simulation ───────────────────────────────────────────────────────────
def should_fill_yes():
    """Simulate whether a YES sell order gets filled this cycle."""
    if SCENARIO == "adverse":
        return fake_cycle[0] % 10 < 9   # 90% YES fills
    if SCENARIO == "skew":
        return True                       # YES always fills, NO never does
    return fake_cycle[0] % 2 == 0        # alternate YES/NO normally

def should_fill_no():
    if SCENARIO == "adverse":
        return fake_cycle[0] % 10 == 0   # 10% NO fills
    if SCENARIO == "skew":
        return False
    return fake_cycle[0] % 2 == 1


# ── Mock response factory ─────────────────────────────────────────────────────
def make_response(status_code=200, data=None):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = data or {}
    r.raise_for_status = MagicMock()
    r.text = json.dumps(data or {})[:120]
    return r


# ── Intercept requests.get ────────────────────────────────────────────────────
def mock_get(url, **kwargs):
    global fake_yes_inv, fake_no_inv, fake_balance

    # Wallet balance
    if "/v1/wallet/assets" in url:
        bal = 0.10 if SCENARIO == "nocash" else fake_balance
        return make_response(200, {"assets": [{"symbol": "USD", "availableBalance": bal}]})

    # List events — return our fake EUR/GBP event
    if "/v1/pm/events" in url and "events" not in url.split("?")[0].split("/")[-1]:
        return make_response(200, {"events": [{
            "id":    FAKE_EVENT_ID,
            "title": "Will EUR/GBP be above £0.8490 by 4:00 PM GMT?",
            "markets": [{"id": FAKE_MARKET_ID, "status": "open"}],
        }]})

    # Get single event (market detail)
    if f"/v1/pm/events/{FAKE_EVENT_ID}" in url:
        mins = fake_minutes_left()
        from datetime import datetime, timezone, timedelta
        close_dt = (datetime.now(timezone.utc) + timedelta(minutes=mins)).isoformat()
        return make_response(200, {"data": {
            "engine": "CLOB",
            "closingDate": close_dt,
            "markets": [{
                "id":            FAKE_MARKET_ID,
                "status":        "open",
                "outcome1Id":    FAKE_YES_ID,
                "outcome2Id":    FAKE_NO_ID,
                "outcome1Price": 0.50,
                "outcome2Price": 0.50,
                "liquidityReward": {"maxSpreadCents": 5, "minNotionalOrderSize": 1, "rewardPool": 100},
            }],
        }})

    # Portfolio / inventory
    if "/v1/pm/portfolio" in url:
        return make_response(200, {"outcomeBalances": [
            {"outcomeId": FAKE_YES_ID, "availableBalance": fake_yes_inv},
            {"outcomeId": FAKE_NO_ID,  "availableBalance": fake_no_inv},
        ]})

    # Get single order — simulate partial/full fills
    for oid, meta in FAKE_ORDER_POOL.items():
        if f"/v1/pm/orders/{oid}" in url:
            return make_response(200, meta)

    # List open orders (cancel_all_open on startup)
    if "/v1/pm/orders" in url:
        return make_response(200, {"orders": []})

    print(f"  [MOCK GET  ] unmatched: {url}")
    return make_response(404, {})


# ── Intercept requests.post ───────────────────────────────────────────────────
def mock_post(url, **kwargs):
    global fake_yes_inv, fake_no_inv, fake_balance

    # External FX feed — return fake rate
    if "frankfurter.app" in url or "er-api.com" in url:
        return make_response(200, {"rates": {"GBP": get_fake_rate()}})

    body = {}
    if kwargs.get("data"):
        try:
            body = json.loads(kwargs["data"])
        except Exception:
            pass

    # Mint
    if "/mint" in url:
        qty = int(body.get("quantity", 1))
        cost = float(qty)
        if fake_balance >= cost:
            fake_balance  -= cost
            fake_yes_inv  += qty
            fake_no_inv   += qty
            print(f"  [MOCK MINT ] {qty} pairs | balance now ${fake_balance:.2f}")
            return make_response(200, {"outcome1Price": 0.50, "outcome2Price": 0.50})
        return make_response(400, {"error": "insufficient balance"})

    # Burn
    if "/burn" in url:
        qty = int(body.get("quantity", 0))
        fake_yes_inv  = max(0.0, fake_yes_inv - qty)
        fake_no_inv   = max(0.0, fake_no_inv  - qty)
        proceeds = float(qty)
        fake_balance += proceeds
        print(f"  [MOCK BURN ] {qty} pairs → ${proceeds:.2f} | balance now ${fake_balance:.2f}")
        return make_response(200, {"proceeds": proceeds})

    # Place order
    if "/orders" in url and "/mint" not in url and "/burn" not in url:
        oid   = str(uuid.uuid4())
        price = float(body.get("price", 0.55))
        side  = body.get("side", "SELL")
        outcome_id = body.get("outcomeId", "")
        label = "YES" if outcome_id == FAKE_YES_ID else "NO"

        # Simulate fill based on scenario
        if side == "SELL":
            fill_it = should_fill_yes() if label == "YES" else should_fill_no()
        else:
            fill_it = False  # residual liquidation orders — treat as open

        filled_size = float(body.get("amount", 1)) / price if fill_it else 0.0
        status = "filled" if fill_it else "open"

        FAKE_ORDER_POOL[oid] = {
            "id":            oid,
            "status":        status,
            "filledSize":    filled_size,
            "remainingSize": 0.0 if fill_it else float(body.get("amount", 1)) / price,
            "avgFillPrice":  price,
            "price":         price,
        }

        if fill_it:
            proceeds = filled_size * price
            fake_balance += proceeds
            if label == "YES":
                fake_yes_inv = max(0.0, fake_yes_inv - filled_size)
            else:
                fake_no_inv  = max(0.0, fake_no_inv  - filled_size)
            print(f"  [MOCK FILL ] SELL {label} {filled_size:.3f}sh @ {price:.3f} → +${proceeds:.4f}")
        else:
            print(f"  [MOCK ORDER] SELL {label} @ {price:.3f} → resting (no fill this cycle)")

        return make_response(200, {"order": {"id": oid, "status": status}})

    print(f"  [MOCK POST ] unmatched: {url}")
    return make_response(404, {})


# ── Intercept requests.delete ─────────────────────────────────────────────────
def mock_delete(url, **kwargs):
    for oid in list(FAKE_ORDER_POOL.keys()):
        if oid in url:
            FAKE_ORDER_POOL.pop(oid, None)
            print(f"  [MOCK CANCEL] {oid[:8]}")
            return make_response(200, {})
    return make_response(200, {})


# ── Intercept time.sleep ──────────────────────────────────────────────────────
_real_sleep = time.sleep
_cycle_stop = [False]

def mock_sleep(seconds):
    fake_cycle[0] += 1
    print(f"\n{'─'*60}")
    print(f"  [HARNESS] Cycle {fake_cycle[0]} complete | sleeping {seconds:.0f}s (skipped)")
    print(f"{'─'*60}")
    if fake_cycle[0] >= MAX_CYCLES:
        print(f"\n[HARNESS] Reached {MAX_CYCLES} cycles — raising KeyboardInterrupt to trigger clean shutdown\n")
        raise KeyboardInterrupt
    _real_sleep(seconds * SPEED_MUL)


# ── Apply scenario pre-conditions ─────────────────────────────────────────────
if SCENARIO == "capreach":
    # Pretend we already spent $48 of the $50 cap
    import importlib, types
    # We'll inject session_mint_cost after import

if SCENARIO == "nocash":
    fake_balance = 0.10


# ── Patch & run ───────────────────────────────────────────────────────────────
with patch("requests.get",    side_effect=mock_get), \
     patch("requests.post",   side_effect=mock_post), \
     patch("requests.delete", side_effect=mock_delete), \
     patch("time.sleep",      side_effect=mock_sleep):

    # Set env before import
    import os
    os.environ.setdefault("PUBLIC_KEY", "pk_test_harness")
    os.environ.setdefault("SECRET_KEY", "sk_test_harness")
    os.environ.setdefault("BASE_URL",   "https://relay.bayse.markets")

    # Import the bot's run() — this will use our patched requests/sleep
    # We import it fresh so globals reset
    import bayse_fx_bot as bot

    if SCENARIO == "capreach":
        bot.session_mint_cost = 48.00
        print("[HARNESS] Pre-injected session_mint_cost=$48.00 (cap test)\n")

    try:
        bot.run()
    except SystemExit:
        pass

print(f"\n{'='*60}")
print(f"  HARNESS COMPLETE  |  scenario={SCENARIO}")
print(f"  Final fake_balance : ${fake_balance:.4f}")
print(f"  Final inventory    : YES={fake_yes_inv:.4f}  NO={fake_no_inv:.4f}")
print(f"  Orders in pool     : {len(FAKE_ORDER_POOL)}")
print(f"{'='*60}\n")
