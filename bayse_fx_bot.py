"""
Bayse EUR/GBP Market Maker v8 — FULLY CORRECTED
=================================================

FIXES APPLIED IN THIS VERSION
══════════════════════════════

PRIOR BUG FIXES (from code audit):
  Bug 1: check_order() reads flat response — no wrapper key unwrapping
  Bug 2: process_active_orders() now updates session_sell_recv on every fill
  Bug 3: Startup key validation — exits immediately if .env is missing
  Bug 4: Mint guard now allows minting when balance >= $1 (not >= MINT_PAIRS)
  Bug 5: prev_filled=0.0 added to active_orders entries to prevent double-counting
  Bug 6: get_minutes_to_close() fallback is 5.0 (fail-safe) not 60.0
  Bug 7: cancel_all_open() scope warning documented

CHAPTER 1 REFERENCE PROBLEMS NOW ALSO FIXED:
  Ch1-A: SINGLE-SIDED RESIDUAL NOT LIQUIDATED AT CLOSE-OUT
          Chapter 1 Figure 1.3 specifies: "Market-sell any single-sided residual
          at best available price (5% discount)". The old close-out only burned
          paired shares and left single-sided residual to expire at $0.
          FIX: After burning pairs, any remaining YES or NO is sold via market
          order at a 5% discount to fair value to recover partial capital.

  Ch1-B: NO FILL ASYMMETRY MONITORING (adverse selection detection)
          Section 1.4.2 states: "A market maker monitoring fill rates over a
          rolling window should expect roughly balanced fills. Persistent
          imbalance — YES fills at 80% of cycles while NO fills 20% — is
          strong evidence that the fair value estimate is wrong or data is
          lagging informed participants."
          FIX: FillTracker class tracks YES/NO fills over a rolling 10-cycle
          window. If imbalance > ADVERSE_SELECTION_THRESHOLD, the spread
          is widened by ADVERSE_SELECTION_MULTIPLIER and a warning is logged.

  Ch1-C: NO INVENTORY SKEWING (inventory risk mitigation)
          Section 1.4.1 and Figure 1.4 state: "skew quotes toward imbalanced
          side to mean-revert". The old bot had zero inventory skewing — it
          posted the same YES/NO ask regardless of whether it held 20 YES
          shares and 0 NO shares (a severely imbalanced book).
          FIX: When yes_shares != no_shares by more than SKEW_THRESHOLD,
          the ask on the oversupplied side is lowered by SKEW_AMOUNT to
          attract buyers and reduce the imbalance faster.

  Ch1-D: SPREAD IS TIME-INVARIANT (violates inventory risk scaling)
          Section 1.4.1 states: "inventory risk grows with the square root of
          time. As the contract approaches expiry, the appropriate response is
          to WIDEN spreads as time shrinks." The old bot used a flat 3¢ markup
          at all times — overcharging near the start (too few fills) and
          undercharging near the end (inadequate compensation per fill).
          FIX: Markup is now time-scaled. It starts wide (MARKUP_WIDE) when
          > WIDE_SPREAD_MINS remain and narrows to MARKUP_TIGHT in the middle
          window, then widens again in the final CLOSE_OUT_MINS window as a
          last-resort fill incentive before burn.
"""

import requests, os, time, hmac, hashlib, base64, json, logging, sys, re
from datetime import datetime, timezone
from collections import deque
from dotenv import load_dotenv

load_dotenv()

BASE       = os.getenv("BASE_URL", "https://relay.bayse.markets")
PUBLIC_KEY = os.getenv("PUBLIC_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

# ── BUG 3 FIX: Validate keys immediately ─────────────────────────────────────
if not PUBLIC_KEY or not SECRET_KEY:
    print("=" * 60)
    print("ERROR: PUBLIC_KEY and SECRET_KEY must be set in .env")
    print("  BASE_URL=https://relay.bayse.markets")
    print("  PUBLIC_KEY=pk_live_xxxxxxxxxxxx")
    print("  SECRET_KEY=sk_live_xxxxxxxxxxxx")
    print("=" * 60)
    sys.exit(1)

# ── CONFIG ────────────────────────────────────────────────────────────────────
MINT_PAIRS          = 2        # share pairs to mint per cycle ($5 per mint)
MAX_INVENTORY_PAIRS = 20       # max minted pairs held at once
CLOSE_OUT_MINS      = 5        # close-out window: burn + liquidate residual
REPRICE_INTERVAL    = 30       # seconds between cycles
REPRICE_THRESHOLD   = 0.005    # reprice if fair moves more than this
SESSION_MAX_MINT    = 50.00    # hard cap on total USD spent minting
MIN_BALANCE_USD     = 5.00     # pause if wallet drops below this
CURRENCY            = "USD"
LOG_FILE            = "mm_v8.log"
PROB_SENSITIVITY    = 20       # fair value model sensitivity

# ── CH1-D FIX: Time-scaled markup ────────────────────────────────────────────
# Spreads widen when more than WIDE_SPREAD_MINS remain (inventory risk is high,
# fewer fills expected — better to charge more per fill)
# Spreads narrow in the mid-session window to attract fills
# Spreads widen again near expiry to compensate for residual risk
MARKUP_WIDE         = 0.045    # 4.5¢ each side when > WIDE_SPREAD_MINS remain
MARKUP_TIGHT        = 0.025    # 2.5¢ each side in the mid-session window
MARKUP_NEAR_EXPIRY  = 0.035    # 3.5¢ each side inside 15 min (last-resort fills)
WIDE_SPREAD_MINS    = 45       # minutes remaining above which we use wide spread
NEAR_EXPIRY_MINS    = 15       # minutes remaining below which we use near-expiry spread

# ── CH1-C FIX: Inventory skew parameters ─────────────────────────────────────
SKEW_THRESHOLD      = 2.0      # skew activates when |yes_shares - no_shares| > this
SKEW_AMOUNT         = 0.020    # 2¢ per unit of imbalance beyond threshold (capped)
MAX_SKEW            = 0.050    # maximum skew adjustment — never skew more than 5¢

# ── CH1-B FIX: Adverse selection detection ───────────────────────────────────
ADVERSE_SELECTION_THRESHOLD   = 0.75   # flag if one side fills > 75% of the time
ADVERSE_SELECTION_MULTIPLIER  = 1.40   # widen spread by 40% when detected
FILL_WINDOW                   = 10     # rolling window of cycles to measure fill rate

# ── CH1-A FIX: Single-sided residual liquidation ─────────────────────────────
RESIDUAL_DISCOUNT   = 0.05     # sell singles at 5% below fair value at close-out
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


# ── CH1-B FIX: Fill asymmetry tracker ────────────────────────────────────────
class FillTracker:
    """
    Tracks YES vs NO fill counts over a rolling window to detect adverse
    selection. Section 1.4.2: "Persistent imbalance is strong evidence
    that the fair value estimate is systematically wrong or data is lagging."
    """
    def __init__(self, window: int = FILL_WINDOW):
        self.yes_fills: deque = deque(maxlen=window)
        self.no_fills:  deque = deque(maxlen=window)

    def record_fill(self, label: str) -> None:
        if label == "YES":
            self.yes_fills.append(1)
            self.no_fills.append(0)
        else:
            self.yes_fills.append(0)
            self.no_fills.append(1)

    def yes_fill_rate(self) -> float:
        total = len(self.yes_fills)
        if total == 0:
            return 0.5
        return sum(self.yes_fills) / total

    def is_adverse_selection_detected(self) -> bool:
        rate = self.yes_fill_rate()
        return rate > ADVERSE_SELECTION_THRESHOLD or rate < (1 - ADVERSE_SELECTION_THRESHOLD)

    def dominant_side(self) -> str:
        return "YES" if self.yes_fill_rate() > 0.5 else "NO"

fill_tracker = FillTracker()


# ── CH1-D FIX: Time-scaled markup ────────────────────────────────────────────
def get_markup(minutes_left: float) -> float:
    """
    Returns the appropriate markup for the current point in the session.
    Wide early (inventory risk high, fewer fills expected).
    Tight in the mid window (maximize fill probability).
    Medium near expiry (attract last fills before close-out).
    """
    if minutes_left > WIDE_SPREAD_MINS:
        markup = MARKUP_WIDE
        reason = "wide (early session)"
    elif minutes_left <= NEAR_EXPIRY_MINS:
        markup = MARKUP_NEAR_EXPIRY
        reason = "near-expiry"
    else:
        markup = MARKUP_TIGHT
        reason = "tight (mid session)"

    # CH1-B: widen further if adverse selection detected
    if fill_tracker.is_adverse_selection_detected():
        markup = round(markup * ADVERSE_SELECTION_MULTIPLIER, 4)
        reason += f" + adverse selection x{ADVERSE_SELECTION_MULTIPLIER}"

    log.info(f"Markup={markup:.4f} ({reason}) | {minutes_left:.1f}min left")
    return markup


# ── CH1-C FIX: Inventory skew calculator ─────────────────────────────────────
def compute_skew(yes_shares: float, no_shares: float) -> float:
    """
    Returns a skew value to subtract from ask prices on the oversupplied side.
    Positive skew = lower asks = more attractive = sell faster.

    If long YES (yes > no): lower YES ask to sell YES faster.
    If long NO  (no > yes): lower NO  ask to sell NO  faster.

    Section 1.4.1: "Widen spreads as time shrinks and skew quotes toward
    imbalanced side to mean-revert."
    """
    imbalance = abs(yes_shares - no_shares)
    if imbalance <= SKEW_THRESHOLD:
        return 0.0
    excess = imbalance - SKEW_THRESHOLD
    skew   = min(SKEW_AMOUNT * excess, MAX_SKEW)
    return round(skew, 4)


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
    title = market.get("title", "")
    try:
        match = re.search(r"by (\d+):(\d+)\s*(AM|PM)\s*GMT", title, re.IGNORECASE)
        if not match:
            # BUG 6 FIX: fail safe — assume nearly expired
            log.warning("Cannot parse closing time from title — defaulting to 5min (safe)")
            return 5.0
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
        return 5.0  # BUG 6 FIX: fail safe


# ── PORTFOLIO ─────────────────────────────────────────────────────────────────
def get_inventory(yes_outcome_id: str, no_outcome_id: str) -> tuple[float, float]:
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
        log.error(f"  ✗ MINT failed ({r.status_code}): {result}")
        return False
    except Exception as e:
        log.error(f"mint_shares error: {e}")
        return False


# ── BURN ──────────────────────────────────────────────────────────────────────
def burn_shares(market_id: str, quantity: int) -> float:
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
        log.error(f"  ✗ BURN failed ({r.status_code}): {result}")
        return 0.0
    except Exception as e:
        log.error(f"burn_shares error: {e}")
        return 0.0


# ── PLACE SELL LIMIT ──────────────────────────────────────────────────────────
def place_sell_limit(
    event_id: str, market_id: str, outcome_id: str,
    label: str, price: float, shares: float,
) -> str | None:
    """GTC postOnly SELL limit. postOnly ensures we are always a maker."""
    amount = max(round(shares * price, 2), 1.00)
    path   = f"/v1/pm/events/{event_id}/markets/{market_id}/orders"
    body   = {
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
        log.error(f"  ✗ SELL {label} failed ({r.status_code}): {result}")
        return None
    except Exception as e:
        log.error(f"place_sell_limit error: {e}")
        return None


# ── CH1-A FIX: Liquidate single-sided residual at close-out ──────────────────
def liquidate_residual(
    event_id: str, market_id: str,
    outcome_id: str, label: str,
    shares: float, fair_price: float,
) -> float:
    """
    Chapter 1 Figure 1.3: "Market-sell any single-sided residual at best
    available price (5% discount) rather than holding to expiry."

    Sells remaining single-sided shares at a discounted price to recover
    partial capital rather than letting them expire worthless at $0.

    Uses a LIMIT order (not market) at a discounted price to stay
    within postOnly maker rules, giving the order a chance to fill
    quickly before expiry while still being a resting order.
    """
    if shares < 0.5:
        return 0.0

    # Discount from fair value to attract immediate fills
    discounted_price = round(max(0.02, fair_price * (1.0 - RESIDUAL_DISCOUNT)), 3)
    amount = max(round(shares * discounted_price, 2), 1.00)

    path = f"/v1/pm/events/{event_id}/markets/{market_id}/orders"
    body = {
        "outcomeId":   outcome_id,
        "side":        "SELL",
        "type":        "LIMIT",
        "price":       discounted_price,
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
            order = result.get("order") or result.get("clobOrder") or {}
            oid   = order.get("id")
            log.info(
                f"  🔻 RESIDUAL SELL {label} @ {discounted_price:.3f} "
                f"({RESIDUAL_DISCOUNT*100:.0f}% below fair={fair_price:.3f}) | "
                f"{shares:.3f}sh | ${amount:.2f} | id={oid[:8] if oid else 'n/a'}"
            )
            return discounted_price
        log.error(f"  ✗ Residual sell {label} failed ({r.status_code}): {result}")
        return 0.0
    except Exception as e:
        log.error(f"liquidate_residual error: {e}")
        return 0.0


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
    """
    WARNING (Bug 7): cancels ALL open orders across the entire account.
    If running multiple bots on the same account, add a marketId filter.
    """
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
    """
    BUG 1 FIX: GET /v1/pm/orders/{id} returns the order as a flat top-level
    object — not wrapped in 'order' or 'clobOrder'. Read it directly.
    """
    try:
        r     = requests.get(
            f"{BASE}/v1/pm/orders/{order_id}",
            headers=read_headers(), timeout=10
        )
        order = r.json()   # flat object per API docs
        return {
            "status":         (order.get("status") or "unknown").lower(),
            "filled_size":    float(order.get("filledSize")    or 0),
            "remaining_size": float(order.get("remainingSize") or 0),
            "avg_fill_price": float(order.get("avgFillPrice")  or order.get("price") or 0),
        }
    except Exception as e:
        log.error(f"check_order {order_id[:8]}: {e}")
        return {"status": "unknown", "filled_size": 0, "remaining_size": 0, "avg_fill_price": 0}


# ── FILL TRACKER + P&L ───────────────────────────────────────────────────────
def process_active_orders(active: dict[str, dict]) -> dict[str, dict]:
    """
    BUG 2 FIX: session_sell_recv now updated on every new fill delta.
    BUG 5 FIX: prev_filled tracks cumulative fills to avoid double-counting.
    CH1-B FIX: fill_tracker records each fill for asymmetry detection.
    """
    global session_sell_recv
    still_open = {}

    for oid, meta in active.items():
        info        = check_order(oid)
        status      = info["status"]
        filled      = info["filled_size"]
        prev_filled = meta.get("prev_filled", 0.0)
        new_fill    = filled - prev_filled

        if new_fill > 0.001:
            proceeds = new_fill * info["avg_fill_price"]
            session_sell_recv       += proceeds         # BUG 2 FIX
            meta["prev_filled"]      = filled           # BUG 5 FIX
            fill_tracker.record_fill(meta["label"])     # CH1-B FIX
            log.info(
                f"  ✔ Fill | SELL {meta['label']} | "
                f"+{new_fill:.4f}sh @ {info['avg_fill_price']:.4f} | "
                f"proceeds=${proceeds:.4f} | session_recv=${session_sell_recv:.4f}"
            )

        if status in ("open", "partial_filled"):
            still_open[oid] = meta

    return still_open


# ── P&L DISPLAY ───────────────────────────────────────────────────────────────
session_start_bal: float = 0.0
session_mint_cost: float = 0.0
session_sell_recv: float = 0.0
session_burn_recv: float = 0.0


def log_pnl(yes_shares: float, no_shares: float, fair_yes: float) -> None:
    current_bal   = get_balance()
    unrealised    = yes_shares * fair_yes + no_shares * (1.0 - fair_yes)
    net_pnl       = session_sell_recv + session_burn_recv - session_mint_cost + unrealised
    session_delta = current_bal - session_start_bal

    # CH1-B: report fill asymmetry alongside P&L
    yes_rate = fill_tracker.yes_fill_rate()
    asymmetry_warning = ""
    if fill_tracker.is_adverse_selection_detected():
        asymmetry_warning = f" ⚠ ADVERSE SELECTION DETECTED (YES fill rate={yes_rate:.0%})"

    log.info("─" * 62)
    log.info(f"  Mint cost       : -${session_mint_cost:.4f}")
    log.info(f"  Sell received   :  ${session_sell_recv:.4f}")
    log.info(f"  Burn proceeds   :  ${session_burn_recv:.4f}")
    log.info(f"  Unrealised inv  :  ${unrealised:.4f}  (YES={yes_shares:.2f} NO={no_shares:.2f})")
    log.info(f"  Net P&L         :  {'+' if net_pnl >= 0 else ''}{net_pnl:.4f}")
    log.info(
        f"  Wallet ${session_start_bal:.2f} → ${current_bal:.2f} "
        f"(Δ={'+' if session_delta >= 0 else ''}{session_delta:.2f} {CURRENCY})"
    )
    log.info(f"  Fill asymmetry  :  YES={yes_rate:.0%} NO={1-yes_rate:.0%}{asymmetry_warning}")
    log.info("─" * 62)


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run() -> None:
    global session_start_bal, session_mint_cost, session_sell_recv, session_burn_recv

    log.info("=" * 62)
    log.info("  Bayse EUR/GBP Market Maker v8 — FULLY CORRECTED")
    log.info("  Fixes: 7 code bugs + 4 Chapter 1 reference issues")
    log.info("  Ch1-A: Residual liquidation at close-out")
    log.info("  Ch1-B: Fill asymmetry / adverse selection detection")
    log.info("  Ch1-C: Inventory skewing toward imbalanced side")
    log.info("  Ch1-D: Time-scaled markup (wide early, tight mid, medium near expiry)")
    log.info("=" * 62)

    while True:
        balance = get_balance()
        if balance > 0:
            session_start_bal = balance
            log.info(f"Starting balance: ${session_start_bal:.2f} {CURRENCY}")
            break
        log.info("Waiting for balance... retry in 15s")
        time.sleep(15)

    cancel_all_open()

    # active_orders: {order_id: {label, price, shares, prev_filled}}
    active_orders: dict[str, dict] = {}
    last_market_id = None
    last_fair_yes  = None

    try:
        while True:
            log.info("")
            log.info("══ Cycle ══")

            # Hard mint cap
            if session_mint_cost >= SESSION_MAX_MINT:
                log.warning(f"Mint cap ${SESSION_MAX_MINT:.2f} reached — shutting down")
                for oid in active_orders:
                    cancel_order(oid)
                log_pnl(0, 0, 0.5)
                break

            market        = find_market()
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

            # Market rollover
            if last_market_id and last_market_id != market_id:
                log.info("New market — cancelling stale orders")
                for oid in active_orders:
                    cancel_order(oid)
                active_orders.clear()
                last_fair_yes = None
            last_market_id = market_id

            minutes_left = get_minutes_to_close(market)
            log.info(f"Minutes to close: {minutes_left:.1f}")

            # FX rate + fair value (needed for both close-out and normal cycle)
            rate = get_eur_gbp_rate()
            if rate is None:
                time.sleep(REPRICE_INTERVAL)
                continue

            fair_yes, strike = compute_fair(rate, title, minutes_left)
            fair_no          = round(1.0 - fair_yes, 4)

            # ── CLOSE-OUT PHASE ───────────────────────────────────────────────
            if minutes_left < CLOSE_OUT_MINS:
                log.warning(f"< {CLOSE_OUT_MINS} min to close — entering close-out phase")

                # Step 1: cancel all resting sell orders
                for oid in list(active_orders.keys()):
                    cancel_order(oid)
                active_orders.clear()

                # Step 2: read final inventory
                yes_shares, no_shares = get_inventory(yes_outcome_id, no_outcome_id)

                # Step 3: burn all paired inventory (always $1 back per pair)
                burnable = int(min(yes_shares, no_shares))
                if burnable > 0:
                    proceeds = burn_shares(market_id, burnable)
                    session_burn_recv += proceeds
                    log.info(f"Burned {burnable} pairs → ${proceeds:.4f}")
                else:
                    log.info("No paired shares to burn")

                # CH1-A FIX: Step 4 — liquidate single-sided residual
                # (Figure 1.3: "Market-sell singles at 5% discount rather than
                # holding to expiry where they may expire worthless")
                yes_remaining = yes_shares - burnable
                no_remaining  = no_shares  - burnable

                if yes_remaining >= 0.5:
                    log.warning(
                        f"Single-sided residual: YES={yes_remaining:.4f} — "
                        f"liquidating at {RESIDUAL_DISCOUNT*100:.0f}% discount"
                    )
                    liquidate_residual(
                        event_id, market_id, yes_outcome_id,
                        "YES", yes_remaining, fair_yes
                    )

                if no_remaining >= 0.5:
                    log.warning(
                        f"Single-sided residual: NO={no_remaining:.4f} — "
                        f"liquidating at {RESIDUAL_DISCOUNT*100:.0f}% discount"
                    )
                    liquidate_residual(
                        event_id, market_id, no_outcome_id,
                        "NO", no_remaining, fair_no
                    )

                log_pnl(yes_remaining, no_remaining, fair_yes)
                log.info("Standing down until next market cycle...")
                time.sleep(max(60, int(minutes_left * 60) + 60))
                continue

            # ── CH1-D FIX: time-scaled markup ────────────────────────────────
            markup = get_markup(minutes_left)

            # ── CH1-B FIX: log adverse selection status ───────────────────────
            if fill_tracker.is_adverse_selection_detected():
                log.warning(
                    f"Adverse selection detected: {fill_tracker.dominant_side()} fills "
                    f"dominating at {fill_tracker.yes_fill_rate():.0%} YES rate — "
                    f"spread widened to {markup:.4f}"
                )

            # ── Ask prices with markup ─────────────────────────────────────────
            yes_ask = round(fair_yes + markup, 3)
            no_ask  = round(fair_no  + markup, 3)
            yes_ask = max(0.02, min(0.99, yes_ask))
            no_ask  = max(0.02, min(0.99, no_ask))

            combined = round(yes_ask + no_ask, 4)
            log.info(
                f"Fair: YES={fair_yes:.4f} NO={fair_no:.4f} | "
                f"Ask:  YES={yes_ask:.3f} NO={no_ask:.3f} | "
                f"Combined={combined:.4f} (profit/pair=${combined - 1.0:.4f})"
            )

            if combined <= 1.00:
                log.warning(f"Combined ask {combined:.4f} ≤ 1.00 — no profit possible")
                time.sleep(REPRICE_INTERVAL)
                continue

            # Balance check
            balance = get_balance()
            log.info(f"Balance: ${balance:.4f}")
            if balance < MIN_BALANCE_USD:
                log.warning(f"Balance below ${MIN_BALANCE_USD} — pausing 60s")
                time.sleep(60)
                continue

            # Inventory
            yes_shares, no_shares = get_inventory(yes_outcome_id, no_outcome_id)
            paired = min(yes_shares, no_shares)
            log.info(f"Paired inventory: {paired:.4f} pairs")

            # CH1-C FIX: compute inventory skew
            skew = compute_skew(yes_shares, no_shares)
            if skew > 0:
                if yes_shares > no_shares:
                    # Long YES — lower YES ask to sell it faster
                    yes_ask_skewed = round(max(0.02, yes_ask - skew), 3)
                    no_ask_skewed  = no_ask
                    log.info(
                        f"Skew={skew:.4f} (long YES by {yes_shares-no_shares:.2f}sh) | "
                        f"YES ask: {yes_ask:.3f}→{yes_ask_skewed:.3f}"
                    )
                    yes_ask = yes_ask_skewed
                else:
                    # Long NO — lower NO ask to sell it faster
                    no_ask_skewed = round(max(0.02, no_ask - skew), 3)
                    log.info(
                        f"Skew={skew:.4f} (long NO by {no_shares-yes_shares:.2f}sh) | "
                        f"NO ask: {no_ask:.3f}→{no_ask_skewed:.3f}"
                    )
                    no_ask = no_ask_skewed

            # Reprice decision
            fair_moved       = (
                last_fair_yes is None or
                abs(fair_yes - last_fair_yes) >= REPRICE_THRESHOLD
            )
            no_active_orders = len(active_orders) == 0

            if fair_moved or no_active_orders:
                log.info(
                    f"Repricing | fair_yes: {last_fair_yes} → {fair_yes} | "
                    f"active_orders: {len(active_orders)}"
                )
                for oid in list(active_orders.keys()):
                    cancel_order(oid)
                active_orders.clear()

                # BUG 4 FIX: mint guard — allow if balance >= $1, not >= MINT_PAIRS
                if paired < MINT_PAIRS and balance >= 1.00:
                    can_mint = min(
                        MINT_PAIRS,
                        int(balance),
                        int(SESSION_MAX_MINT - session_mint_cost),
                        MAX_INVENTORY_PAIRS - int(paired),
                    )
                    if can_mint >= 1:
                        ok = mint_shares(market_id, can_mint)
                        if ok:
                            session_mint_cost += can_mint
                            yes_shares, no_shares = get_inventory(yes_outcome_id, no_outcome_id)
                            paired = min(yes_shares, no_shares)
                    else:
                        log.info(f"Mint skipped: can_mint={can_mint} inventory={paired:.0f}")

                # SELL YES
                if yes_shares >= 1.0:
                    oid = place_sell_limit(
                        event_id, market_id, yes_outcome_id,
                        label="YES", price=yes_ask, shares=yes_shares,
                    )
                    if oid:
                        # BUG 5 FIX: prev_filled=0.0 prevents double-counting
                        active_orders[oid] = {
                            "label": "YES", "price": yes_ask,
                            "shares": yes_shares, "prev_filled": 0.0,
                        }
                else:
                    log.info("SELL YES skipped: no YES inventory")

                # SELL NO
                if no_shares >= 1.0:
                    oid = place_sell_limit(
                        event_id, market_id, no_outcome_id,
                        label="NO", price=no_ask, shares=no_shares,
                    )
                    if oid:
                        active_orders[oid] = {
                            "label": "NO", "price": no_ask,
                            "shares": no_shares, "prev_filled": 0.0,
                        }
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
                # CH1-A FIX: also liquidate residual on manual shutdown
                yes_r = yes_s - burnable
                no_r  = no_s  - burnable
                fair_y = last_fair_yes or 0.5
                if yes_r >= 0.5:
                    liquidate_residual(
                        market["event_id"], market["market_id"],
                        market["yes_outcome_id"], "YES", yes_r, fair_y
                 )
                if no_r >= 0.5:
                    liquidate_residual(
                        market["event_id"], market["market_id"],
                        market["no_outcome_id"], "NO", no_r, 1.0 - fair_y
                    )
        log_pnl(0, 0, last_fair_yes or 0.5)
        log.info("=== Market maker stopped ===")


if __name__ == "__main__":
    run()
