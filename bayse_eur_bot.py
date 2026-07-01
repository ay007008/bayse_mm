"""
Bayse EUR/GBP Market Maker v8
================================
CORRECT STRATEGY (Mint/Burn + CLOB Limit Orders):

  1. MINT N share pairs  → costs $N, receive N YES + N NO shares
  2. Post SELL limit on YES above fair  (e.g. fair=0.35 → ask 0.38)
  3. Post SELL limit on NO  above fair  (e.g. fair=0.65 → ask 0.68)
  4. If both sell: collected 0.38+0.68 = $1.06 on $1.00 cost = 6¢ profit
  5. If one side sells and the other doesn't, BURN the matched pairs
     to recover capital, keeping only the sold-side profit.

PROFIT SOURCES:
  - Spread earned when both YES and NO sells fill above mint cost ($1.00)
  - Bayse liquidity reward pool (two-sided resting quotes score highest)

RISK:
  - Market resolves before NO sell fills → holding worthless NO shares
  - Mitigated by: tight time window (1hr), burn on close, fair-value pricing

KEY DESIGN:
  - No BUY orders placed — inventory comes from MINT only
  - postOnly=True on all SELL orders — we are always the maker
  - Burn residual paired inventory at CLOSE_OUT_MINS
  - Session max spend cap on mint calls
"""

import requests, os, time, hmac, hashlib, base64, json, logging, sys, re, math
from datetime import datetime, timezone
import requests
import time
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
load_dotenv()

BASE       = os.getenv("BASE_URL", "https://relay.bayse.markets")
PUBLIC_KEY = os.getenv("PUBLIC_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

# ── CONFIG ────────────────────────────────────────────────────────────────────
MINT_PAIRS          = 3        # share pairs to mint per cycle ($5 per mint)
YES_MARKUP          = 0.03     # sell YES at fair_yes + this markup
NO_MARKUP           = 0.03     # sell NO  at fair_no  + this markup
MIN_MARKUP          = 0.02     # abort if we can't get at least this over fair
MAX_INVENTORY_PAIRS = 20       # max unminted pairs held at once
CLOSE_OUT_MINS      = 5        # burn all residual inventory this close to expiry
REPRICE_INTERVAL    = 30       # seconds between cycles
REPRICE_THRESHOLD   = 0.005    # only reprice if fair moves by more than 0.5c
SESSION_MAX_MINT    = 50.00    # hard cap on total USD spent minting
MIN_BALANCE_USD     = 5.00     # pause if wallet balance drops below this
CURRENCY            = "USD"
LOG_FILE            = "mm_v8.log"
PROB_SENSITIVITY    = 20       # fair value model sensitivity
# ─────────────────────────────────────────────────────────────────────────────


# ── LOGGING ───────────────────────────────────────────────────────────────────
class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

log = logging.getLogger()
log.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(message)s")
fh  = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
sh  = FlushHandler(sys.stdout)
sh.setFormatter(fmt)
log.addHandler(fh)
log.addHandler(sh)


# ── AUTH ──────────────────────────────────────────────────────────────────────
def read_headers() -> dict:
    return {"X-Public-Key": PUBLIC_KEY, "Content-Type": "application/json"}

def write_headers(method: str, path: str, body: dict | None) -> dict:
    timestamp = str(int(time.time()))
    body_str  = json.dumps(body, separators=(",", ":"), sort_keys=False) if body else ""
    body_hash = hashlib.sha256(body_str.encode()).hexdigest() if body_str else ""
    payload   = f"{timestamp}.{method.upper()}.{path}.{body_hash}"
    signature = base64.b64encode(
        hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "X-Public-Key": PUBLIC_KEY,
        "X-Timestamp":  timestamp,
        "X-Signature":  signature,
        "Content-Type": "application/json",
    }

def sign_request(secret_key: str, payload: str) -> str:
    """
    Generate HMAC-SHA256 signature for an API request payload.

    Args:
        secret_key: API secret key string
        payload: JSON-serialised request body string

    Returns:
        Base64-encoded HMAC-SHA256 signature string
    """
    sig_bytes = hmac.new(
        key=secret_key.encode('utf-8'),
        msg=payload.encode('utf-8'),
        digestmod=hashlib.sha256
    ).digest()
    return base64.b64encode(sig_bytes).decode('utf-8')


# ── WALLET ────────────────────────────────────────────────────────────────────
def get_balance() -> float:
    try:
        r = requests.get(f"{BASE}/v1/wallet/assets", headers=read_headers(), timeout=10)
        r.raise_for_status()
        for asset in r.json().get("assets", []):
            if asset.get("symbol") == CURRENCY:
                return float(asset.get("availableBalance", 0))
        return 0.0
    except Exception as e:
        log.error(f"get_balance error: {e}")
        return 0.0


# ── MARKET DISCOVERY ──────────────────────────────────────────────────────────
def fetch_market_detail(event_id: str, title: str) -> dict | None:
    try:
        r = requests.get(f"{BASE}/v1/pm/events/{event_id}", headers=read_headers(), timeout=10)
        r.raise_for_status()
        raw   = r.json()
        event = raw.get("data") or raw.get("event") or raw

        engine = (event.get("engine") or "AMM").upper()
        if engine != "CLOB":
            log.info(f"Event '{title}' uses {engine} — skipping (CLOB only)")
            return None

        for market in event.get("markets", []):
            if (market.get("status") or "").lower() != "open":
                continue

            lr               = market.get("liquidityReward") or {}
            max_spread_cents = float(lr.get("maxSpreadCents", 5))
            min_notional     = float(lr.get("minNotionalOrderSize", 1))
            reward_pool      = float(lr.get("rewardPool", 0))

            return {
                "event_id":         event_id,
                "market_id":        market.get("id"),
                "yes_outcome_id":   market.get("outcome1Id"),
                "no_outcome_id":    market.get("outcome2Id"),
                "yes_price":        float(market.get("outcome1Price") or 0.5),
                "no_price":         float(market.get("outcome2Price") or 0.5),
                "title":            title,
                "engine":           engine,
                "closing_date":     event.get("closingDate"),
                "reward_pool":      reward_pool,
                "max_spread_cents": max_spread_cents,
                "min_notional":     min_notional,
            }

        log.warning(f"Event {event_id}: no open market")
        return None
    except Exception as e:
        log.error(f"fetch_market_detail error: {e}")
        return None


def find_market() -> dict | None:
    try:
        r = requests.get(
            f"{BASE}/v1/pm/events",
            params={"keyword": "EUR", "status": "open", "page": 1, "size": 50},
            headers=read_headers(), timeout=10,
        )
        r.raise_for_status()
        events = r.json().get("events") or r.json().get("data") or []
        for event in events:
            title = (event.get("title") or "").upper()
            if "EUR" in title and "GBP" in title:
                for market in event.get("markets", []):
                    if (market.get("status") or "").lower() == "open":
                        log.info(f"Market: '{event.get('title')}'")
                        return fetch_market_detail(event.get("id"), event.get("title"))
        log.warning("No open EUR/GBP CLOB market found")
        return None
    except Exception as e:
        log.error(f"find_market error: {e}")
        return None


# ── LIVE FX RATE ──────────────────────────────────────────────────────────────
def get_eur_gbp_rate() -> float | None:
    for url, label in [
        ("https://api.frankfurter.app/latest?from=EUR&to=GBP", "frankfurter"),
        ("https://open.er-api.com/v6/latest/EUR",              "er-api"),
    ]:
        try:
            r = requests.get(url, timeout=8)
            r.raise_for_status()
            rate = float(r.json()["rates"]["GBP"])
            log.info(f"EUR/GBP = {rate:.5f}  [{label}]")
            return rate
        except Exception:
            continue
    log.error("get_eur_gbp_rate: all sources failed")
    return None


# ── FAIR VALUE ────────────────────────────────────────────────────────────────
def compute_fair(rate: float, title: str, minutes_left: float) -> tuple[float, float | None]:
    """
    Returns (fair_yes, strike).
    fair_yes = probability EUR/GBP ends above strike.
    fair_no  = 1 - fair_yes  (caller computes this).
    """
    match = re.search(r"£?(0\.\d+)", title)
    if not match:
        log.warning("Could not parse strike — defaulting fair to 0.50")
        return 0.50, None

    strike      = float(match.group(1))
    distance    = strike - rate
    time_scale  = min(1.0, 5.0 / max(minutes_left, 1))
    sensitivity = PROB_SENSITIVITY * (0.3 + 0.7 * time_scale)
    fair_yes    = max(0.05, min(0.95, round(0.50 - distance * sensitivity, 4)))

    log.info(
        f"Strike={strike:.5f} | Rate={rate:.5f} | dist={distance:+.5f} | "
        f"sens={sensitivity:.1f} | fair_yes={fair_yes:.4f} fair_no={1-fair_yes:.4f}"
    )
    return fair_yes, strike


# ── TIME TO CLOSE ─────────────────────────────────────────────────────────────
def get_minutes_to_close(market: dict) -> float:
    closing_date = market.get("closing_date")
    if closing_date:
        try:
            close_dt = datetime.fromisoformat(closing_date.replace("Z", "+00:00"))
            diff = (close_dt - datetime.now(timezone.utc)).total_seconds() / 60
            return max(0.0, diff)
        except Exception:
            pass
    # Fallback: parse title
    title = market.get("title", "")
    try:
        match = re.search(r"by (\d+):(\d+)\s*(AM|PM)\s*GMT", title, re.IGNORECASE)
        if not match:
            return 60.0
        hour, minute, period = int(match.group(1)), int(match.group(2)), match.group(3).upper()
        if period == "PM" and hour != 12: hour += 12
        if period == "AM" and hour == 12: hour  = 0
        now_utc    = datetime.now(timezone.utc).replace(tzinfo=None)
        close_time = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
        diff       = (close_time - now_utc).total_seconds() / 60
        if diff < -30: diff += 24 * 60
        return max(0.0, diff)
    except Exception as e:
        log.error(f"get_minutes_to_close parse error: {e}")
        return 60.0


# ── ORDER BOOK ────────────────────────────────────────────────────────────────
def get_best_bids(yes_outcome_id: str, no_outcome_id: str) -> tuple[float | None, float | None]:
    """
    Return (best_buy_price_for_yes, best_buy_price_for_no) from the live book.
    These are the highest prices someone is willing to BUY at —
    i.e. what we'd get if we placed a market sell RIGHT NOW.
    We use this to validate our ask prices are realistic.
    """
    try:
        r = requests.get(
            f"{BASE}/v1/pm/books",
            params={"outcomeId[]": [yes_outcome_id, no_outcome_id], "depth": 3, "currency": CURRENCY},
            headers=read_headers(), timeout=10,
        )
        r.raise_for_status()
        books = r.json()
        yes_best_bid = no_best_bid = None
        for book in books:
            bids = book.get("bids", [])
            best = bids[0]["price"] if bids else None
            if book.get("outcomeId") == yes_outcome_id:
                yes_best_bid = best
            elif book.get("outcomeId") == no_outcome_id:
                no_best_bid = best
        log.info(f"Best bids → YES:{yes_best_bid} | NO:{no_best_bid}")
        return yes_best_bid, no_best_bid
    except Exception as e:
        log.error(f"get_best_bids error: {e}")
        return None, None


# ── POSITION ──────────────────────────────────────────────────────────────────
def get_inventory(yes_outcome_id: str, no_outcome_id: str) -> tuple[float, float]:
    """
    Returns (yes_shares, no_shares) available to sell.
    Uses availableBalance from portfolio as documented.
    """
    yes_shares = no_shares = 0.0
    try:
        r = requests.get(f"{BASE}/v1/pm/portfolio", headers=read_headers(), timeout=10)
        r.raise_for_status()
        for b in r.json().get("outcomeBalances", []):
            oid    = b.get("outcomeId")
            shares = float(b.get("availableBalance") or b.get("balance") or 0)
            if oid == yes_outcome_id:
                yes_shares = shares
            elif oid == no_outcome_id:
                no_shares = shares
    except Exception as e:
        log.error(f"get_inventory error: {e}")
    log.info(f"Inventory: YES={yes_shares:.4f} | NO={no_shares:.4f} shares")
    return yes_shares, no_shares




# ── MINT ──────────────────────────────────────────────────────────────────────
def mint_shares(market_id: str, quantity: int) -> bool:
    """
    Mint `quantity` YES+NO share pairs. Costs quantity × $1.00.
    Returns True on success.
    """
    path = f"/v1/pm/markets/{market_id}/mint"
    body = {"quantity": quantity, "currency": CURRENCY}
    try:
        body_str = json.dumps(body, separators=(",", ":"), sort_keys=False)
        r = requests.post(
            f"{BASE}{path}", data=body_str,
            headers=write_headers("POST", path, body), timeout=10,
        )
        result = r.json()
        if r.status_code in (200, 201):
            log.info(
                f"  ✔ MINT {quantity} pairs (cost=${quantity:.2f}) | "
                f"yes_price={result.get('outcome1Price')} "
                f"no_price={result.get('outcome2Price')}"
            )
            return True
        else:
            log.error(f"  ✗ MINT failed ({r.status_code}): {result}")
            return False
    except Exception as e:
        log.error(f"mint_shares error: {e}")
        return False


# ── BURN ──────────────────────────────────────────────────────────────────────
def burn_shares(market_id: str, quantity: int) -> float:
    """
    Burn `quantity` YES+NO share pairs. Returns proceeds (≈ quantity × $1.00).
    Call this when we need to reduce inventory or close out before expiry.
    """
    if quantity <= 0:
        return 0.0
    path = f"/v1/pm/markets/{market_id}/burn"
    body = {"quantity": quantity, "currency": CURRENCY}
    try:
        body_str = json.dumps(body, separators=(",", ":"), sort_keys=False)
        r = requests.post(
            f"{BASE}{path}", data=body_str,
            headers=write_headers("POST", path, body), timeout=10,
        )
        result = r.json()
        if r.status_code in (200, 201):
            proceeds = float(result.get("proceeds", quantity))
            log.info(f"  🔥 BURN {quantity} pairs → received ${proceeds:.4f}")
            return proceeds
        else:
            log.error(f"  ✗ BURN failed ({r.status_code}): {result}")
            return 0.0
    except Exception as e:
        log.error(f"burn_shares error: {e}")
        return 0.0


# ── PLACE SELL LIMIT ──────────────────────────────────────────────────────────
def place_sell_limit(
    event_id: str,
    market_id: str,
    outcome_id: str,
    label: str,
    price: float,
    shares: float,
) -> str | None:
    """
    Place a GTC postOnly SELL limit order for `shares` worth of notional.
    `amount` = notional in USD = shares × price (as required by the API).
    postOnly ensures we never cross the spread accidentally.
    """
    amount = round(shares * price, 2)
    amount = max(amount, 1.00)   # API minimum is $1

    path = f"/v1/pm/events/{event_id}/markets/{market_id}/orders"
    body = {
        "outcomeId":   outcome_id,
        "side":        "SELL",
        "type":        "LIMIT",
        "price":       round(price, 3),
        "amount":      amount,
        "currency":    CURRENCY,
        "timeInForce": "GTC",
        "postOnly":    True,
    }
    try:
        body_str = json.dumps(body, separators=(",", ":"), sort_keys=False)
        r = requests.post(
            f"{BASE}{path}", data=body_str,
            headers=write_headers("POST", path, body), timeout=10,
        )
        result = r.json()
        if r.status_code in (200, 201):
            order  = result.get("order") or result.get("clobOrder") or {}
            oid    = order.get("id")
            status = order.get("status", "?")
            log.info(
                f"  → SELL {label} @ {price:.3f} | {shares:.3f}sh | ${amount:.2f} | "
                f"id={oid[:8] if oid else 'n/a'} status={status}"
            )
            return oid
        else:
            log.error(f"  ✗ SELL {label} failed ({r.status_code}): {result}")
            return None
    except Exception as e:
        log.error(f"place_sell_limit error: {e}")
        return None


# ── CANCEL ORDER ──────────────────────────────────────────────────────────────
def cancel_order(order_id: str) -> None:
    path = f"/v1/pm/orders/{order_id}"
    try:
        r = requests.delete(
            f"{BASE}{path}",
            headers=write_headers("DELETE", path, None), timeout=10,
        )
        if r.status_code in (200, 204):
            log.info(f"  ✗ Cancelled {order_id[:8]}")
        else:
            log.warning(f"  Cancel {order_id[:8]} → {r.status_code}: {r.text[:120]}")
    except Exception as e:
        log.error(f"cancel_order error: {e}")


def cancel_all_open() -> None:
    try:
        r = requests.get(
            f"{BASE}/v1/pm/orders",
            params={"status": "open", "size": 50},
            headers=read_headers(), timeout=10,
        )
        r.raise_for_status()
        orders = r.json().get("orders") or r.json().get("data") or []
        if not orders:
            log.info("No stale open orders on startup")
            return
        log.info(f"Cancelling {len(orders)} stale open orders...")
        for o in orders:
            if o.get("id"):
                cancel_order(o["id"])
    except Exception as e:
        log.error(f"cancel_all_open error: {e}")


# ── ORDER STATUS CHECK ────────────────────────────────────────────────────────
def check_order(order_id: str) -> dict:
    try:
        r    = requests.get(f"{BASE}/v1/pm/orders/{order_id}", headers=read_headers(), timeout=10)
        data = r.json()
        order = data.get("order") or data.get("clobOrder") or data
        return {
            "status":         (order.get("status") or "unknown").lower(),
            "filled_size":    float(order.get("filledSize")   or 0),
            "remaining_size": float(order.get("remainingSize") or 0),
            "avg_fill_price": float(order.get("avgFillPrice") or order.get("price") or 0),
        }
    except Exception as e:
        log.error(f"check_order {order_id[:8]}: {e}")
        return {"status": "unknown", "filled_size": 0, "remaining_size": 0, "avg_fill_price": 0}


def process_active_orders(active: dict[str, dict]) -> dict[str, dict]:
    """
    active = {order_id: {label, price, shares}}
    Removes filled/cancelled/expired orders. Returns still-open subset.
    """
    still_open = {}
    for oid, meta in active.items():
        info   = check_order(oid)
        status = info["status"]
        filled = info["filled_size"]
        if filled > 0:
            log.info(
                f"  ✔ Fill | SELL {meta['label']} | "
                f"{filled:.4f}sh @ {info['avg_fill_price']:.4f} | status={status}"
            )
        if status in ("open", "partial_filled"):
            still_open[oid] = meta
        # filled / cancelled / rejected / expired → drop silently
    return still_open


# ── P&L DISPLAY ───────────────────────────────────────────────────────────────
session_start_bal:  float = 0.0
session_mint_cost:  float = 0.0
session_sell_recv:  float = 0.0
session_burn_recv:  float = 0.0


def log_pnl(yes_shares: float, no_shares: float, fair_yes: float) -> None:
    current_bal   = get_balance()
    # Mark inventory at fair value
    unrealised    = yes_shares * fair_yes + no_shares * (1 - fair_yes)
    net_pnl       = session_sell_recv + session_burn_recv - session_mint_cost + unrealised
    session_delta = current_bal - session_start_bal
    log.info("─" * 60)
    log.info(f"  Mint cost       : -${session_mint_cost:.4f}")
    log.info(f"  Sell received   :  ${session_sell_recv:.4f}")
    log.info(f"  Burn proceeds   :  ${session_burn_recv:.4f}")
    log.info(f"  Unrealised inv  :  ${unrealised:.4f}  (YES={yes_shares:.2f} NO={no_shares:.2f})")
    log.info(f"  Net P&L         :  {'+' if net_pnl >= 0 else ''}{net_pnl:.4f}")
    log.info(
        f"  Wallet ${session_start_bal:.2f} → ${current_bal:.2f} "
        f"(Δ={'+' if session_delta >= 0 else ''}{session_delta:.2f} {CURRENCY})"
    )
    log.info("─" * 60)


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run() -> None:
    global session_start_bal, session_mint_cost, session_sell_recv, session_burn_recv

    log.info("=" * 60)
    log.info("  Bayse EUR/GBP Market Maker v8")
    log.info("  Strategy: MINT → SELL YES + SELL NO → profit the spread")
    log.info("  Burn residual pairs to recover capital before expiry")
    log.info("=" * 60)

    # Wait for wallet
    while True:
        balance = get_balance()
        if balance > 0:
            session_start_bal = balance
            log.info(f"Starting balance: ${session_start_bal:.2f} {CURRENCY}")
            break
        log.info("Waiting for balance... retry in 15s")
        time.sleep(15)

    cancel_all_open()

    # active_orders: {order_id: {label, price, shares}}
    active_orders: dict[str, dict] = {}

    last_market_id  = None
    last_fair_yes   = None

    try:
        while True:
            log.info("")
            log.info("══ Cycle ══")

            # ── Hard mint cap ─────────────────────────────────────────────────
            if session_mint_cost >= SESSION_MAX_MINT:
                log.warning(
                    f"Mint spend ${session_mint_cost:.2f} hit cap "
                    f"${SESSION_MAX_MINT:.2f} — shutting down"
                )
                for oid in active_orders:
                    cancel_order(oid)
                log_pnl(0, 0, 0.5)
                break

            # ── Find market ───────────────────────────────────────────────────
            market = find_market()

            # ── Poll existing sell orders ─────────────────────────────────────
            active_orders = process_active_orders(active_orders)

            if not market:
                log.warning("No market — waiting 15s")
                time.sleep(15)
                continue

            event_id       = market["event_id"]
            market_id      = market["market_id"]
            yes_outcome_id = market["yes_outcome_id"]
            no_outcome_id  = market["no_outcome_id"]
            title          = market["title"]

            # ── Market rollover ───────────────────────────────────────────────
            if last_market_id and last_market_id != market_id:
                log.info("New market — cancelling stale orders")
                for oid in active_orders:
                    cancel_order(oid)
                active_orders.clear()
                last_fair_yes = None
            last_market_id = market_id

            # ── Time check ────────────────────────────────────────────────────
            minutes_left = get_minutes_to_close(market)
            log.info(f"Minutes to close: {minutes_left:.1f}")

            # ── CLOSE-OUT: burn all paired inventory before expiry ────────────
            if minutes_left < CLOSE_OUT_MINS:
                log.warning(f"< {CLOSE_OUT_MINS} min to close — burning residual inventory")
                for oid in list(active_orders.keys()):
                    cancel_order(oid)
                active_orders.clear()

                yes_shares, no_shares = get_inventory(yes_outcome_id, no_outcome_id)
                # Burn matched pairs (can only burn min of the two sides)
                burnable = int(min(yes_shares, no_shares))
                if burnable > 0:
                    proceeds = burn_shares(market_id, burnable)
                    session_burn_recv += proceeds
                    log.info(f"Burned {burnable} pairs, received ${proceeds:.4f}")
                else:
                    log.info("No burnable pairs")

                log_pnl(max(0, yes_shares - burnable), max(0, no_shares - burnable), 0.5)
                log.info(f"Standing down until next market cycle...")
                time.sleep(max(60, int(minutes_left * 60) + 60))
                continue

            # ── FX rate + fair value ──────────────────────────────────────────
            rate = get_eur_gbp_rate()
            if rate is None:
                time.sleep(REPRICE_INTERVAL)
                continue

            fair_yes, strike = compute_fair(rate, title, minutes_left)
            fair_no          = round(1.0 - fair_yes, 4)

            # ── Ask prices (what we SELL at, above fair) ──────────────────────
            yes_ask = round(fair_yes + YES_MARKUP, 3)
            no_ask  = round(fair_no  + NO_MARKUP,  3)

            # Clamp to valid range
            yes_ask = max(0.02, min(0.99, yes_ask))
            no_ask  = max(0.02, min(0.99, no_ask))

            # Sanity: yes_ask + no_ask must be > 1.00 to be profitable
            combined = round(yes_ask + no_ask, 4)
            log.info(
                f"Fair: YES={fair_yes:.4f} NO={fair_no:.4f} | "
                f"Ask: YES={yes_ask:.3f} NO={no_ask:.3f} | "
                f"Combined={combined:.4f} (profit/pair=${combined-1:.4f})"
            )
            if combined <= 1.00:
                log.warning(
                    f"Combined ask {combined:.4f} ≤ 1.00 — no profit possible. "
                    f"Increase YES_MARKUP or NO_MARKUP."
                )
                time.sleep(REPRICE_INTERVAL)
                continue

            # ── Balance check ─────────────────────────────────────────────────
            balance = get_balance()
            log.info(f"Balance: ${balance:.4f}")
            if balance < MIN_BALANCE_USD:
                log.warning(f"Balance below ${MIN_BALANCE_USD} — pausing 60s")
                time.sleep(60)
                continue

            # ── Current inventory ─────────────────────────────────────────────
            yes_shares, no_shares = get_inventory(yes_outcome_id, no_outcome_id)
            # Paired inventory = shares we hold on BOTH sides
            paired = min(yes_shares, no_shares)
            log.info(f"Paired inventory: {paired:.4f} pairs")

            # ── Reprice decision ──────────────────────────────────────────────
            fair_moved = (
                last_fair_yes is None or
                abs(fair_yes - last_fair_yes) >= REPRICE_THRESHOLD
            )
            no_active_orders = len(active_orders) == 0

            if fair_moved or no_active_orders:
                # Cancel and re-post
                for oid in list(active_orders.keys()):
                    cancel_order(oid)
                active_orders.clear()

                # ── Mint if inventory is low ──────────────────────────────────
                if paired < MINT_PAIRS and balance >= MINT_PAIRS:
                    can_mint = min(
                        MINT_PAIRS,
                        int(balance),
                        int(SESSION_MAX_MINT - session_mint_cost),
                    )
                    if can_mint > 0 and paired + can_mint <= MAX_INVENTORY_PAIRS:
                        ok = mint_shares(market_id, can_mint)
                        if ok:
                            session_mint_cost += can_mint
                            # Refresh inventory after mint
                            yes_shares, no_shares = get_inventory(yes_outcome_id, no_outcome_id)
                            paired = min(yes_shares, no_shares)
                    else:
                        log.info(
                            f"Mint skipped: can_mint={can_mint} "
                            f"inventory={paired:.0f} cap={MAX_INVENTORY_PAIRS}"
                        )

                # ── Post SELL limit on YES ────────────────────────────────────
                if yes_shares >= 1.0:
                    oid = place_sell_limit(
                        event_id, market_id, yes_outcome_id,
                        label="YES", price=yes_ask, shares=yes_shares,
                    )
                    if oid:
                        active_orders[oid] = {"label": "YES", "price": yes_ask, "shares": yes_shares}
                else:
                    log.info("SELL YES skipped: no YES inventory")

                # ── Post SELL limit on NO ─────────────────────────────────────
                if no_shares >= 1.0:
                    oid = place_sell_limit(
                        event_id, market_id, no_outcome_id,
                        label="NO", price=no_ask, shares=no_shares,
                    )
                    if oid:
                        active_orders[oid] = {"label": "NO", "price": no_ask, "shares": no_shares}
                else:
                    log.info("SELL NO skipped: no NO inventory")

                last_fair_yes = fair_yes

            else:
                log.info(f"Fair stable (Δ={abs(fair_yes - last_fair_yes):.4f}) — keeping orders")

            log.info(f"Active sell orders: {[o[:8] for o in active_orders]}")
            log_pnl(yes_shares, no_shares, fair_yes)
            log.info(f"Sleeping {REPRICE_INTERVAL}s...")
            time.sleep(REPRICE_INTERVAL)

    except KeyboardInterrupt:
        log.info("Interrupted — cancelling all orders and burning inventory...")
        for oid in active_orders:
            cancel_order(oid)
        # Final burn of any residual paired inventory
        if last_market_id:
            market = find_market()
            if market:
                yes_s, no_s = get_inventory(
                    market["yes_outcome_id"], market["no_outcome_id"]
                )
                burnable = int(min(yes_s, no_s))
                if burnable > 0:
                    proceeds = burn_shares(market["market_id"], burnable)
                    session_burn_recv += proceeds
        log_pnl(0, 0, last_fair_yes or 0.5)
        log.info("=== Market maker stopped ===")


if __name__ == "__main__":
    run()
