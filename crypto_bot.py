"""
Bayse SOL/USDT 15-Min Market Maker  —  v6
==========================================
Changes from v5:

  1. Binary Black-Scholes pricer
       compute_fair() now uses the exact closed-form formula for a
       cash-or-nothing binary call option:
           P(UP wins) = N(d2)
           d2 = [ln(S/K) + (r - ½σ²)τ] / (σ√τ)
       where:
           S  = current SOL spot price
           K  = market threshold (strike)
           r  = 0 (no risk-free rate needed for a prediction market)
           σ  = live annualised volatility (from Binance 1-min klines)
           τ  = time remaining as fraction of 1 year

       The binary call gives the exact risk-neutral probability that SOL
       will be ABOVE the threshold at expiry — which is exactly what the
       UP outcome pays $1 on.

  2. Multi-level order spreading (close / near / far / extreme)
       Instead of placing one large limit bid on each side, the bot now
       spreads the total notional across four price levels per side.
       Each level has its own:
           • price offset below fair value  (configured below)
           • notional allocation fraction   (must sum to 1.0)
           • minimum quantity filter        (skip if below min_notional)

       Level definitions (offsets are added ON TOP of the base markup):
           close   : base_markup + 0.000  — 40 % of notional
           near    : base_markup + 0.010  — 30 % of notional
           far     : base_markup + 0.025  — 20 % of notional
           extreme : base_markup + 0.050  — 10 % of notional

       Rationale: staggered bids prevent a single informed aggressor from
       sweeping the full position in one fill. Levels closer to fair value
       get more size; extreme levels act as insurance.

       Adverse selection / divergence guards apply PER LEVEL — if UP is
       paused, all four UP levels are skipped.

  3. All v5 features retained unchanged:
       • Binance spot + perp WebSocket feeds
       • Market divergence guard (divergence_threshold = 0.15)
       • Early adverse scram (adverse_min_fills = 2)
       • Inventory skewing (per-level bid prices are skewed individually)
       • Sliding liquidation markdown at close-out
       • Per-side circuit breaker (max_side_inventory_usd = 3.0)
       • Stop-loss (stop_loss_usd = 15.0)
       • Live volatility from Binance 1-min klines (refreshed per market)
"""

import requests, os, time, hmac, hashlib, base64, json, logging, sys, math, threading
from datetime import datetime, timezone
from collections import deque
from dataclasses import dataclass, field
from dotenv import load_dotenv

try:
    import websocket
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────

@dataclass
class Config:
    base_url:   str = os.getenv("BASE_URL",   "https://relay.bayse.markets")
    public_key: str = os.getenv("PUBLIC_KEY", "")
    secret_key: str = os.getenv("SECRET_KEY", "")
    currency:   str = "USD"
    log_file:   str = "sol_mm_v6.log"

    # Cycle
    reprice_interval_s: int   = 5
    reprice_threshold:  float = 0.010

    # Capital
    session_max_notional:   float = 100.0
    min_balance_usd:        float = 5.0
    close_out_mins:         float = 5.0
    max_side_inventory_usd: float = 3.0

    # ── Multi-level spread config ─────────────────────────────────────────
    # Each level: (extra_offset_above_base_markup, notional_fraction)
    # The base markup is computed by get_markup() as before.
    # extra_offset is how much FURTHER below fair value to bid at that level.
    # notional_fraction must sum to 1.0 across all levels.
    # The total notional per side = bid_amount_usd (split across levels).
    level_close_offset:    float = 0.000   # closest to fair value
    level_near_offset:     float = 0.010
    level_far_offset:      float = 0.025
    level_extreme_offset:  float = 0.050   # deepest discount

    level_close_fraction:   float = 0.40   # 40% of notional at close level
    level_near_fraction:    float = 0.30   # 30% at near
    level_far_fraction:     float = 0.20   # 20% at far
    level_extreme_fraction: float = 0.10   # 10% at extreme

    # Base bid sizing (total per side before splitting across levels)
    bid_amount_usd: float = 4.00
    min_bid_price:  float = 0.03
    max_bid_price:  float = 0.97

    # Spread
    markup_wide:      float = 0.075
    markup_mid:       float = 0.065
    markup_floor:     float = 0.058
    wide_spread_mins: float = 10.0
    near_expiry_mins: float = 5.0

    # Adverse selection
    adverse_threshold:  float = 0.65
    adverse_multiplier: float = 1.50
    fill_window:        int   = 4
    adverse_min_fills:  int   = 2

    # Market divergence guard
    divergence_threshold: float = 0.15

    # Inventory skewing
    skew_factor: float = 0.005
    max_skew:    float = 0.040

    # Stop-loss
    stop_loss_usd: float = 15.0

    # Fair value (Binary Black-Scholes)
    sol_annual_vol: float = 0.80   # fallback if live vol fetch fails
    risk_free_rate: float = 0.00   # r=0 for prediction markets


cfg = Config()

if not cfg.public_key or not cfg.secret_key:
    print("=" * 60)
    print("ERROR: PUBLIC_KEY and SECRET_KEY must be set in .env")
    print("=" * 60)
    sys.exit(1)

# Derive the four spread levels as a list for easy iteration
SPREAD_LEVELS = [
    ("close",   cfg.level_close_offset,   cfg.level_close_fraction),
    ("near",    cfg.level_near_offset,    cfg.level_near_fraction),
    ("far",     cfg.level_far_offset,     cfg.level_far_fraction),
    ("extreme", cfg.level_extreme_offset, cfg.level_extreme_fraction),
]


# ── LOGGING ───────────────────────────────────────────────────────────────────

class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

log = logging.getLogger("sol_mm_v6")
log.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(message)s")
fh  = logging.FileHandler(cfg.log_file, encoding="utf-8")
fh.setFormatter(fmt)
sh  = FlushHandler(sys.stdout)
sh.setFormatter(fmt)
log.addHandler(fh)
log.addHandler(sh)


# ── PRICE FEED — Binance spot WebSocket + perp futures WebSocket ─────────────

class _WSFeed:
    """Generic thread-safe WebSocket price feed."""
    def __init__(self, url: str, label: str):
        self._price: float | None = None
        self._lock  = threading.Lock()
        self._label = label
        if WS_AVAILABLE:
            self._start(url)
        else:
            log.warning(f"websocket-client not installed — {label} feed unavailable")

    def _start(self, url: str) -> None:
        def on_message(ws, msg):
            try:
                with self._lock:
                    self._price = float(json.loads(msg)["p"])
            except Exception:
                pass

        def on_error(ws, err):
            log.warning(f"{self._label} WS error: {err}")

        def run():
            while True:
                try:
                    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error)
                    ws.run_forever(ping_interval=20)
                except Exception as e:
                    log.warning(f"{self._label} WS reconnect in 5s: {e}")
                time.sleep(5)

        threading.Thread(target=run, daemon=True).start()
        log.info(f"{self._label} WebSocket feed started")

    def get_price(self) -> float | None:
        with self._lock:
            return self._price


_spot_feed = _WSFeed(
    "wss://stream.binance.com:9443/ws/solusdt@aggTrade",
    "Binance-spot",
)
_perp_feed = _WSFeed(
    "wss://fstream.binance.com/ws/solusdt@aggTrade",
    "Binance-perp",
)


def get_sol_price() -> float | None:
    p = _spot_feed.get_price()
    if p:
        log.info(f"SOL/USD = ${p:.4f}  [binance-spot-ws]")
        return p
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "SOLUSDT"}, timeout=6,
        )
        r.raise_for_status()
        p = float(r.json()["price"])
        log.info(f"SOL/USD = ${p:.4f}  [binance-spot-rest]")
        return p
    except Exception:
        pass
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana", "vs_currencies": "usd"}, timeout=8,
        )
        r.raise_for_status()
        p = float(r.json()["solana"]["usd"])
        log.warning(f"SOL/USD = ${p:.4f}  [coingecko — STALE]")
        return p
    except Exception:
        pass
    log.error("get_sol_price: all sources failed")
    return None


def get_perp_signal(spot_price: float) -> tuple[float, str]:
    perp = _perp_feed.get_price()
    if perp is None or spot_price <= 0:
        return 0.0, "no-perp-data"
    premium_pct = (perp - spot_price) / spot_price * 100
    if premium_pct > 0.10:
        signal = "UP-pressure"
    elif premium_pct < -0.10:
        signal = "DN-pressure"
    else:
        signal = "neutral"
    log.info(
        f"Perp=${perp:.4f} | spot=${spot_price:.4f} | "
        f"premium={premium_pct:+.3f}% | signal={signal}"
    )
    return premium_pct, signal


# ── FILL TRACKER ──────────────────────────────────────────────────────────────

class FillTracker:
    def __init__(self):
        self.up_fills: deque = deque(maxlen=cfg.fill_window)

    def record_fill(self, label: str) -> None:
        self.up_fills.append(1 if label.startswith("UP") else 0)

    def up_fill_rate(self) -> float:
        return sum(self.up_fills) / len(self.up_fills) if self.up_fills else 0.5

    def fill_count(self) -> int:
        return len(self.up_fills)

    def is_adverse(self) -> bool:
        count = self.fill_count()
        if count == 0:
            return False
        rate = self.up_fill_rate()
        if count >= cfg.fill_window:
            return rate > cfg.adverse_threshold or rate < (1.0 - cfg.adverse_threshold)
        if count >= cfg.adverse_min_fills:
            return rate >= 1.0 or rate <= 0.0
        return False

    def dominant_side(self) -> str:
        return "UP" if self.up_fill_rate() > 0.5 else "DN"

    def reset(self) -> None:
        self.up_fills.clear()


fill_tracker = FillTracker()


# ── AUTH ──────────────────────────────────────────────────────────────────────

def read_headers() -> dict:
    return {"X-Public-Key": cfg.public_key, "Content-Type": "application/json"}


def write_headers(method: str, path: str, body: dict | None) -> dict:
    timestamp = str(int(time.time()))
    body_str  = json.dumps(body, separators=(",", ":"), sort_keys=False) if body else ""
    body_hash = hashlib.sha256(body_str.encode()).hexdigest() if body_str else ""
    payload   = f"{timestamp}.{method.upper()}.{path}.{body_hash}"
    sig = base64.b64encode(
        hmac.new(cfg.secret_key.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "X-Public-Key": cfg.public_key,
        "X-Timestamp":  timestamp,
        "X-Signature":  sig,
        "Content-Type": "application/json",
    }


# ── WALLET ────────────────────────────────────────────────────────────────────

def get_balance() -> float:
    try:
        r = requests.get(f"{cfg.base_url}/v1/wallet/assets", headers=read_headers(), timeout=10)
        r.raise_for_status()
        for asset in r.json().get("assets", []):
            if asset.get("symbol") == cfg.currency:
                return float(asset.get("availableBalance", 0))
        return 0.0
    except Exception as e:
        log.error(f"get_balance error: {e}")
        return 0.0


# ── MARKET DISCOVERY ──────────────────────────────────────────────────────────

def find_sol_market() -> dict | None:
    for slug in ["crypto-sol-15min", "sol-15min", "solana-15min"]:
        try:
            r = requests.get(
                f"{cfg.base_url}/v1/pm/series/{slug}/events",
                params={"status": "open", "page": 1, "size": 5},
                headers=read_headers(), timeout=10,
            )
            if r.status_code == 200:
                for event in (r.json().get("events") or r.json().get("data") or []):
                    detail = _fetch_market_detail(event.get("id"), event.get("title", ""))
                    if detail:
                        return detail
        except Exception:
            pass

    try:
        r = requests.get(
            f"{cfg.base_url}/v1/pm/events",
            params={"keyword": "Solana", "status": "open", "page": 1, "size": 30},
            headers=read_headers(), timeout=10,
        )
        r.raise_for_status()
        for event in (r.json().get("events") or r.json().get("data") or []):
            title = (event.get("title") or "").upper()
            if "SOL" in title and ("15" in title or "MINUTE" in title):
                detail = _fetch_market_detail(event.get("id"), event.get("title", ""))
                if detail:
                    return detail
        log.warning("No open SOL 15min market found")
        return None
    except Exception as e:
        log.error(f"find_sol_market error: {e}")
        return None


def _fetch_market_detail(event_id: str, title: str) -> dict | None:
    try:
        r   = requests.get(f"{cfg.base_url}/v1/pm/events/{event_id}", headers=read_headers(), timeout=10)
        raw = r.json()
        engine          = (raw.get("engine") or "CLOB").upper()
        event_threshold = float(raw.get("eventThreshold") or 0)
        log.info(f"Event '{title}' engine={engine}")
        for market in (raw.get("markets") or []):
            if (market.get("status") or "").lower() != "open":
                continue
            threshold    = float(market.get("marketThreshold") or event_threshold or 0)
            fee_rate     = float(market.get("feePercentage", 10)) / 100.0
            lr           = market.get("liquidityReward") or {}
            min_notional = float(lr.get("minNotionalOrderSize", 3))
            up_price     = float(market.get("outcome1Price") or 0.5)
            dn_price     = float(market.get("outcome2Price") or 0.5)
            log.info(
                f"  threshold=${threshold:.4f} | fee={fee_rate:.0%} | "
                f"min_notional=${min_notional:.2f} | up={up_price:.4f} dn={dn_price:.4f}"
            )
            return {
                "event_id":      event_id,
                "market_id":     market.get("id"),
                "up_outcome_id": market.get("outcome1Id"),
                "dn_outcome_id": market.get("outcome2Id"),
                "threshold":     threshold,
                "fee_rate":      fee_rate,
                "min_notional":  min_notional,
                "title":         title,
                "engine":        engine,
                "closing_date":  raw.get("closingDate"),
                "market_up":     up_price,
                "market_dn":     dn_price,
            }
        log.warning(f"Event {event_id}: no open market")
        return None
    except Exception as e:
        log.error(f"_fetch_market_detail error: {e}")
        return None


# ── FAIR VALUE — Binary Black-Scholes (Cash-or-Nothing Digital Call) ──────────
#
# A prediction market UP outcome pays $1 if SOL > threshold at expiry.
# This is exactly a cash-or-nothing binary CALL option with payoff $1.
#
# Closed-form price under Black-Scholes:
#
#   P(UP) = N(d2)
#
#   d2 = [ ln(S/K) + (r - ½σ²)·τ ] / (σ·√τ)
#
# where:
#   S  = current SOL spot price
#   K  = market threshold (strike price)
#   r  = risk-free rate (0.0 for prediction markets — no financing cost)
#   σ  = annualised volatility (live from Binance klines, fallback to cfg)
#   τ  = time to expiry in years
#        = minutes_left / (365 × 24 × 60)
#
# The DN outcome pays $1 if SOL ≤ threshold, so:
#   P(DN) = 1 - N(d2) = N(-d2)
#
# Key properties vs the old approach:
#   • Old: approximated N(z) using a Horner polynomial on a hand-rolled z-score
#          that scaled drift by vol/time. Correct concept, imprecise implementation.
#   • New: textbook binary option formula — the de facto standard in financial
#          mathematics for pricing exactly this kind of digital payout.
#   • The (r - ½σ²)·τ term is the Itô correction — it accounts for the fact
#          that under log-normal diffusion, the expected LOG price drifts at
#          r - ½σ² rather than r. Omitting it (as the old code effectively did)
#          introduces a small but systematic mispricing that grows with σ and τ.
#
# Sanity clamp: [0.05, 0.95] — no outcome trades at absolute certainty.

def _standard_normal_cdf(z: float) -> float:
    """
    Abramowitz & Stegun rational approximation — maximum error < 7.5e-8.
    Used for N(d2) in the binary Black-Scholes formula.
    """
    if z < -8.0: return 0.0
    if z >  8.0: return 1.0
    if z < 0:
        return 1.0 - _standard_normal_cdf(-z)
    k    = 1.0 / (1.0 + 0.2316419 * z)
    poly = k * (0.319381530
           + k * (-0.356563782
           + k * (1.781477937
           + k * (-1.821255978
           + k * 1.330274429))))
    pdf_val = math.exp(-0.5 * z * z) / math.sqrt(2.0 * math.pi)
    return 1.0 - pdf_val * poly


def compute_fair(sol_price: float, threshold: float, minutes_left: float) -> float:
    """
    Binary Black-Scholes fair value for the UP outcome.

    Returns P(SOL > threshold at expiry) using the cash-or-nothing
    digital call formula: P = N(d2).

    Clamped to [0.05, 0.95] to prevent degenerate quotes near expiry.
    """
    if threshold <= 0 or sol_price <= 0:
        return 0.50

    annual_vol = _live_vol if _live_vol > 0 else cfg.sol_annual_vol
    r          = cfg.risk_free_rate

    # τ in years — minimum 1 second to avoid division by zero
    tau = max(minutes_left / (365.0 * 24.0 * 60.0), 1.0 / (365.0 * 24.0 * 60.0))

    # d2: the Black-Scholes distance-to-strike with Itô correction
    log_moneyness = math.log(sol_price / threshold)           # ln(S/K)
    ito_drift     = (r - 0.5 * annual_vol ** 2) * tau         # (r - ½σ²)τ
    d2            = (log_moneyness + ito_drift) / (annual_vol * math.sqrt(tau))

    # Cash-or-nothing call price = N(d2)
    fair_up = _standard_normal_cdf(d2)
    fair_up = max(0.05, min(0.95, round(fair_up, 4)))

    log.info(
        f"BBS | S=${sol_price:.4f} K=${threshold:.4f} "
        f"τ={tau*365*24*60:.1f}min σ={annual_vol:.4f} r={r:.4f} | "
        f"ln(S/K)={log_moneyness:+.6f} d2={d2:+.4f} | "
        f"fair_up={fair_up:.4f} fair_dn={1-fair_up:.4f}"
    )
    return fair_up


# ── LIVE VOLATILITY ───────────────────────────────────────────────────────────

_live_vol: float = 0.0


def get_live_vol(symbol: str = "SOLUSDT", interval: str = "1m", lookback: int = 60) -> float:
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": lookback},
            timeout=8,
        )
        r.raise_for_status()
        candles = r.json()
        closes  = [float(k[4]) for k in candles]
        if len(closes) < 2:
            raise ValueError("Not enough candles")
        log_returns = [math.log(closes[i] / closes[i-1]) for i in range(1, len(closes))]
        n           = len(log_returns)
        mean        = sum(log_returns) / n
        variance    = sum((x - mean) ** 2 for x in log_returns) / (n - 1)
        std_per_min = math.sqrt(variance)
        annual_vol  = std_per_min * math.sqrt(525_600)   # √(365×24×60)
        annual_vol  = max(0.20, min(3.00, annual_vol))
        log.info(f"Live vol: {annual_vol:.4f} annualised ({n} 1-min returns on {symbol})")
        return round(annual_vol, 4)
    except Exception as e:
        log.warning(f"get_live_vol failed ({e}) — using fallback {cfg.sol_annual_vol}")
        return cfg.sol_annual_vol


def refresh_vol() -> None:
    global _live_vol
    _live_vol = get_live_vol()


def get_markup(minutes_left: float) -> tuple[float, str]:
    if minutes_left > cfg.wide_spread_mins or minutes_left <= cfg.near_expiry_mins:
        markup, reason = cfg.markup_wide, "wide"
    else:
        markup, reason = cfg.markup_mid, "mid"
    if fill_tracker.is_adverse():
        markup  = round(markup * cfg.adverse_multiplier, 4)
        reason += "+adverse"
    markup = max(markup, cfg.markup_floor)
    log.info(f"Markup={markup:.4f} ({reason}) | {minutes_left:.1f}min left")
    return markup, reason


def check_market_divergence(
    fair_up: float,
    market_up: float,
    market_dn: float,
    perp_signal: str,
) -> tuple[bool, bool, str]:
    skip_up = False
    skip_dn = False
    reasons = []
    up_gap  = abs(market_up - fair_up)
    dn_gap  = abs(market_dn - (1.0 - fair_up))
    if market_up > fair_up + cfg.divergence_threshold:
        skip_dn = True
        reasons.append(f"market_up={market_up:.3f} >> fair={fair_up:.3f} (gap={up_gap:.3f})")
    if market_dn > (1.0 - fair_up) + cfg.divergence_threshold:
        skip_up = True
        reasons.append(f"market_dn={market_dn:.3f} >> fair_dn={1-fair_up:.3f} (gap={dn_gap:.3f})")
    if perp_signal == "UP-pressure" and skip_up:
        reasons.append("perp confirms DN pressure")
    if perp_signal == "DN-pressure" and skip_dn:
        reasons.append("perp confirms UP pressure")
    if skip_up and skip_dn:
        reasons.append("both sides diverged — skipping all bids")
    reason_str = " | ".join(reasons) if reasons else "ok"
    if skip_up or skip_dn:
        log.warning(f"Divergence guard: skip_up={skip_up} skip_dn={skip_dn} | {reason_str}")
    return skip_up, skip_dn, reason_str


# ── SLIDING LIQUIDATION MARKDOWN ─────────────────────────────────────────────

def get_liquidation_markdown(minutes_left: float) -> float:
    if minutes_left >= 5:  return 0.05
    if minutes_left >= 3:  return 0.10
    if minutes_left >= 1:  return 0.20
    return 0.35


# ── INVENTORY SKEWING ─────────────────────────────────────────────────────────

def compute_skewed_bids(
    up_bid: float, dn_bid: float,
    up_shares: float, dn_shares: float,
) -> tuple[float, float]:
    imbalance = up_shares - dn_shares
    if abs(imbalance) < 1.0:
        return up_bid, dn_bid
    skew = min(abs(imbalance) * cfg.skew_factor, cfg.max_skew)
    if imbalance > 0:
        up_bid_out = round(max(cfg.min_bid_price, up_bid - skew), 3)
        dn_bid_out = dn_bid
        log.info(
            f"Inventory skew: +UP {imbalance:.1f}sh → UP bid "
            f"{up_bid:.3f} → {up_bid_out:.3f} (-{skew:.3f})"
        )
    else:
        up_bid_out = up_bid
        dn_bid_out = round(max(cfg.min_bid_price, dn_bid - skew), 3)
        log.info(
            f"Inventory skew: +DN {abs(imbalance):.1f}sh → DN bid "
            f"{dn_bid:.3f} → {dn_bid_out:.3f} (-{skew:.3f})"
        )
    return up_bid_out, dn_bid_out


# ── TIME TO CLOSE ─────────────────────────────────────────────────────────────

def get_minutes_to_close(market: dict) -> float:
    closing_date = market.get("closing_date")
    if closing_date:
        try:
            close_dt = datetime.fromisoformat(closing_date.replace("Z", "+00:00"))
            diff     = (close_dt - datetime.now(timezone.utc)).total_seconds() / 60
            return max(0.0, diff)
        except Exception:
            pass
    log.warning("Cannot parse closing time — defaulting to 2min (fail-safe)")
    return 2.0


# ── INVENTORY ─────────────────────────────────────────────────────────────────

def get_inventory(up_outcome_id: str, dn_outcome_id: str) -> tuple[float, float]:
    up_shares = dn_shares = 0.0
    try:
        r = requests.get(f"{cfg.base_url}/v1/pm/portfolio", headers=read_headers(), timeout=10)
        r.raise_for_status()
        for b in r.json().get("outcomeBalances", []):
            oid    = b.get("outcomeId")
            shares = float(b.get("availableBalance") or b.get("balance") or 0)
            if oid == up_outcome_id:
                up_shares = shares
            elif oid == dn_outcome_id:
                dn_shares = shares
    except Exception as e:
        log.error(f"get_inventory error: {e}")
    log.info(f"Inventory: UP={up_shares:.4f} | DN={dn_shares:.4f} shares")
    return up_shares, dn_shares


# ── BURN ──────────────────────────────────────────────────────────────────────

def burn_shares(market_id: str, quantity: int) -> float:
    if quantity <= 0:
        return 0.0
    path = f"/v1/pm/markets/{market_id}/burn"
    body = {"quantity": quantity, "currency": cfg.currency}
    try:
        body_str = json.dumps(body, separators=(",", ":"), sort_keys=False)
        r = requests.post(
            f"{cfg.base_url}{path}", data=body_str,
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


# ── MULTI-LEVEL PLACE BID ─────────────────────────────────────────────────────
#
# Spreads one side's total notional across four price levels:
#
#   Level     Offset above base markup    Fraction of total notional
#   ──────────────────────────────────────────────────────────────
#   close     +0.000                      40 %
#   near      +0.010                      30 %
#   far       +0.025                      20 %
#   extreme   +0.050                      10 %
#
# Each level gets its own limit order posted at:
#   bid_price = fair_price - base_markup - level_offset
#
# Inventory skew is applied to the BASE bid price; the level offsets are
# applied AFTER skewing so each level preserves its relative depth.
#
# A level is skipped if its allocated notional falls below min_notional
# (the exchange minimum), preventing rejected tiny orders.
#
# Returns a dict of {order_id: order_meta} for the new active orders.

def place_multilevel_bids(
    event_id: str, market_id: str, outcome_id: str,
    side_label: str,
    base_bid_price: float,        # fair - base_markup (already skewed)
    total_notional: float,        # cfg.bid_amount_usd
    min_notional: float,
) -> dict:
    """
    Places up to 4 limit orders for one side at staggered price levels.
    Returns a dict of {order_id: meta} for all successfully placed orders.
    """
    new_orders = {}
    log.info(
        f"  Spreading {side_label} bids | base_bid={base_bid_price:.3f} "
        f"total=${total_notional:.2f} | {len(SPREAD_LEVELS)} levels"
    )

    for level_name, extra_offset, fraction in SPREAD_LEVELS:
        level_notional = round(total_notional * fraction, 2)
        if level_notional < min_notional:
            log.info(
                f"    SKIP {side_label}/{level_name}: notional "
                f"${level_notional:.2f} < min ${min_notional:.2f}"
            )
            continue

        # Each successive level bids further below fair value
        level_price = round(
            max(cfg.min_bid_price, min(cfg.max_bid_price, base_bid_price - extra_offset)),
            3,
        )

        oid = _place_single_bid(
            event_id, market_id, outcome_id,
            label=f"{side_label}/{level_name}",
            bid_price=level_price,
            amount=level_notional,
        )
        if oid:
            new_orders[oid] = {
                "label":       side_label,          # "UP" or "DN" — for fill tracker
                "level":       level_name,
                "price":       level_price,
                "amount":      level_notional,
                "prev_filled": 0.0,
            }

    return new_orders


def _place_single_bid(
    event_id: str, market_id: str, outcome_id: str,
    label: str, bid_price: float, amount: float,
) -> str | None:
    path = f"/v1/pm/events/{event_id}/markets/{market_id}/orders"
    body = {
        "outcomeId":   outcome_id,
        "side":        "BUY",
        "type":        "LIMIT",
        "price":       round(bid_price, 3),
        "amount":      amount,
        "currency":    cfg.currency,
        "timeInForce": "GTC",
        "postOnly":    True,
    }
    try:
        body_str = json.dumps(body, separators=(",", ":"), sort_keys=False)
        r = requests.post(
            f"{cfg.base_url}{path}", data=body_str,
            headers=write_headers("POST", path, body), timeout=10,
        )
        result = r.json()
        if r.status_code in (200, 201):
            order  = result.get("order") or result.get("clobOrder") or {}
            oid    = order.get("id")
            status = order.get("status", "?")
            log.info(
                f"    → BID {label} @ {bid_price:.3f} | ${amount:.2f} | "
                f"id={oid[:8] if oid else 'n/a'} status={status}"
            )
            return oid
        if r.status_code == 422:
            log.warning(f"    ⚠ BID {label} @ {bid_price:.3f} rejected (bid too high or postOnly)")
        else:
            log.error(f"    ✗ BID {label} failed ({r.status_code}): {result}")
        return None
    except Exception as e:
        log.error(f"_place_single_bid error: {e}")
        return None


# ── RESIDUAL LIQUIDATION ──────────────────────────────────────────────────────

def liquidate_residual(
    event_id: str, market_id: str, outcome_id: str,
    label: str, shares: float, fair_price: float,
    min_notional: float, minutes_left: float,
) -> None:
    if shares < 0.5:
        return
    markdown   = get_liquidation_markdown(minutes_left)
    discounted = round(max(0.03, fair_price * (1.0 - markdown)), 3)
    amount     = max(round(shares * discounted, 2), min_notional)
    path       = f"/v1/pm/events/{event_id}/markets/{market_id}/orders"
    body = {
        "outcomeId":   outcome_id,
        "side":        "SELL",
        "type":        "LIMIT",
        "price":       discounted,
        "amount":      amount,
        "currency":    cfg.currency,
        "timeInForce": "GTC",
        "postOnly":    True,
    }
    try:
        body_str = json.dumps(body, separators=(",", ":"), sort_keys=False)
        r = requests.post(
            f"{cfg.base_url}{path}", data=body_str,
            headers=write_headers("POST", path, body), timeout=10,
        )
        result = r.json()
        if r.status_code in (200, 201):
            order = result.get("order") or {}
            oid   = order.get("id")
            log.info(
                f"  🔻 RESIDUAL SELL {label} @ {discounted:.3f} "
                f"({markdown:.0%} below fair={fair_price:.3f}) | "
                f"{shares:.3f}sh | id={oid[:8] if oid else 'n/a'}"
            )
        else:
            log.error(f"  ✗ Residual sell {label} failed ({r.status_code}): {result}")
    except Exception as e:
        log.error(f"liquidate_residual error: {e}")


# ── CANCEL ────────────────────────────────────────────────────────────────────

def cancel_order(order_id: str) -> None:
    path = f"/v1/pm/orders/{order_id}"
    try:
        r = requests.delete(
            f"{cfg.base_url}{path}",
            headers=write_headers("DELETE", path, None), timeout=10,
        )
        if r.status_code in (200, 204):
            log.info(f"  ✗ Cancelled {order_id[:8]}")
        else:
            log.warning(f"  Cancel {order_id[:8]} → {r.status_code}")
    except Exception as e:
        log.error(f"cancel_order error: {e}")


def cancel_all_open() -> None:
    try:
        r = requests.get(
            f"{cfg.base_url}/v1/pm/orders",
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


# ── ORDER STATUS & FILL PROCESSING ────────────────────────────────────────────

def check_order(order_id: str) -> dict:
    try:
        r = requests.get(
            f"{cfg.base_url}/v1/pm/orders/{order_id}",
            headers=read_headers(), timeout=10,
        )
        order = r.json()
        return {
            "status":         (order.get("status") or "unknown").lower(),
            "filled_size":    float(order.get("filledSize")    or 0),
            "avg_fill_price": float(order.get("avgFillPrice")  or order.get("price") or 0),
        }
    except Exception as e:
        log.error(f"check_order {order_id[:8]}: {e}")
        return {"status": "unknown", "filled_size": 0, "avg_fill_price": 0}


def process_active_orders(active: dict) -> dict:
    global session_bid_cost
    still_open = {}
    for oid, meta in active.items():
        info        = check_order(oid)
        filled      = info["filled_size"]
        prev_filled = meta.get("prev_filled", 0.0)
        new_fill    = filled - prev_filled
        if new_fill > 0.001:
            cost              = new_fill * info["avg_fill_price"]
            session_bid_cost += cost
            meta["prev_filled"] = filled
            fill_tracker.record_fill(meta["label"])   # "UP" or "DN"
            log.info(
                f"  ✔ Fill | BID {meta['label']}/{meta.get('level','?')} | "
                f"+{new_fill:.4f}sh @ {info['avg_fill_price']:.4f} | "
                f"cost=${cost:.4f} | session_bid_cost=${session_bid_cost:.4f}"
            )
        if info["status"] in ("open", "partial_filled"):
            still_open[oid] = meta
    return still_open


# ── P&L ───────────────────────────────────────────────────────────────────────

session_start_bal: float = 0.0
session_bid_cost:  float = 0.0
session_sell_recv: float = 0.0
session_burn_recv: float = 0.0
market_history:    list  = []


def compute_net_pnl(up_shares: float, dn_shares: float, fair_up: float) -> float:
    unrealised = up_shares * fair_up + dn_shares * (1.0 - fair_up)
    return session_sell_recv + session_burn_recv - session_bid_cost + unrealised


def log_pnl(up_shares: float, dn_shares: float, fair_up: float, active_orders: dict) -> None:
    current_bal   = get_balance()
    unrealised    = up_shares * fair_up + dn_shares * (1.0 - fair_up)
    net_pnl       = session_sell_recv + session_burn_recv - session_bid_cost + unrealised
    delta         = current_bal - session_start_bal
    up_rate       = fill_tracker.up_fill_rate()
    open_notional = sum(o.get("amount", cfg.bid_amount_usd) for o in active_orders.values())
    adv_warn      = f" ⚠ ADVERSE ({fill_tracker.dominant_side()} dominant)" if fill_tracker.is_adverse() else ""

    log.info("─" * 62)
    log.info(f"  Bid cost (filled)  : -${session_bid_cost:.4f}")
    log.info(f"  Sell received      :  ${session_sell_recv:.4f}")
    log.info(f"  Burn proceeds      :  ${session_burn_recv:.4f}")
    log.info(f"  Unrealised inv     :  ${unrealised:.4f}  (UP={up_shares:.2f} DN={dn_shares:.2f})")
    log.info(f"  Open bid notional  :  ${open_notional:.2f}")
    log.info(f"  Net P&L            :  {'+' if net_pnl >= 0 else ''}{net_pnl:.4f}")
    log.info(
        f"  Wallet ${session_start_bal:.2f} → ${current_bal:.2f} "
        f"(Δ={'+' if delta >= 0 else ''}{delta:.2f} {cfg.currency})"
    )
    log.info(f"  Fill asymmetry     :  UP={up_rate:.0%} DN={1-up_rate:.0%}{adv_warn}")
    log.info("─" * 62)


# ── STOP-LOSS ─────────────────────────────────────────────────────────────────

def check_stop_loss(net_pnl: float, active_orders: dict, market: dict | None) -> bool:
    if net_pnl >= -cfg.stop_loss_usd:
        return False
    log.error(f"🛑 STOP-LOSS: net_pnl=${net_pnl:.4f} < -${cfg.stop_loss_usd:.2f} — shutting down")
    for oid in list(active_orders.keys()):
        cancel_order(oid)
    active_orders.clear()
    if market:
        global session_burn_recv
        up_s, dn_s = get_inventory(market["up_outcome_id"], market["dn_outcome_id"])
        burnable   = int(min(up_s, dn_s))
        if burnable > 0:
            proceeds = burn_shares(market["market_id"], burnable)
            session_burn_recv += proceeds
        up_r, dn_r = up_s - burnable, dn_s - burnable
        if up_r >= 0.5:
            liquidate_residual(
                market["event_id"], market["market_id"], market["up_outcome_id"],
                "UP", up_r, 0.5, market["min_notional"], 0.0,
            )
        if dn_r >= 0.5:
            liquidate_residual(
                market["event_id"], market["market_id"], market["dn_outcome_id"],
                "DN", dn_r, 0.5, market["min_notional"], 0.0,
            )
    log_pnl(0, 0, 0.5, {})
    log.error("=== Stop-loss shutdown complete ===")
    return True


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run() -> None:
    global session_start_bal, session_bid_cost, session_sell_recv, session_burn_recv

    log.info("=" * 62)
    log.info("  Bayse SOL/USDT 15-Min Market Maker  v6")
    log.info("  Binary Black-Scholes pricer | 4-level bid spreading")
    log.info("  Spot WS + Perp WS | Divergence guard | Early adverse scram")
    log.info(f"  markup_mid={cfg.markup_mid} | max_side=${cfg.max_side_inventory_usd} | sleep={cfg.reprice_interval_s}s")
    log.info(f"  close_out={cfg.close_out_mins}min | adverse_thresh={cfg.adverse_threshold} | stop_loss=${cfg.stop_loss_usd}")
    log.info(f"  divergence_threshold={cfg.divergence_threshold} | adverse_min_fills={cfg.adverse_min_fills}")
    log.info(f"  Spread levels: {[(l, f'offset={o:.3f}', f'frac={f:.0%}') for l, o, f in SPREAD_LEVELS]}")
    log.info("=" * 62)

    while True:
        balance = get_balance()
        if balance > 0:
            session_start_bal = balance
            log.info(f"Starting balance: ${session_start_bal:.2f} {cfg.currency}")
            break
        log.info("Waiting for balance... retry in 15s")
        time.sleep(15)

    cancel_all_open()
    refresh_vol()
    last_market_id: str | None  = None
    last_fair_up:   float | None = None
    market:         dict | None  = None
    active_orders:  dict         = {}

    try:
        while True:
            log.info("")
            log.info("══ Cycle ══")

            active_orders = process_active_orders(active_orders)

            market = find_sol_market()
            if not market:
                log.warning("No SOL 15min market — waiting 20s")
                time.sleep(20)
                continue

            event_id     = market["event_id"]
            market_id    = market["market_id"]
            up_outcome   = market["up_outcome_id"]
            dn_outcome   = market["dn_outcome_id"]
            threshold    = market["threshold"]
            fee_rate     = market["fee_rate"]
            min_notional = market["min_notional"]

            # ── Market rollover ───────────────────────────────────────────────
            if last_market_id and last_market_id != market_id:
                log.info("New 15min market — resetting P&L counters")
                for oid in active_orders:
                    cancel_order(oid)
                active_orders.clear()
                last_fair_up = None
                fill_tracker.reset()
                prior_pnl = compute_net_pnl(0, 0, 0.5)
                market_history.append({
                    "market_id":  last_market_id,
                    "net_pnl":    prior_pnl,
                    "fill_rate":  fill_tracker.up_fill_rate(),
                    "timestamp":  datetime.now(timezone.utc).isoformat(),
                })
                log.info(
                    f"Market {last_market_id[:8]} closed | "
                    f"net_pnl={prior_pnl:+.4f} | "
                    f"total markets={len(market_history)}"
                )
                session_bid_cost  = 0.0
                session_sell_recv = 0.0
                session_burn_recv = 0.0
                session_start_bal = get_balance()
                refresh_vol()

            last_market_id = market_id

            minutes_left = get_minutes_to_close(market)
            log.info(f"Minutes to close: {minutes_left:.1f} | threshold=${threshold:.4f}")

            if threshold <= 0:
                log.warning("threshold=0 — retrying in 10s")
                time.sleep(10)
                continue

            sol_price = get_sol_price()
            if sol_price is None:
                time.sleep(cfg.reprice_interval_s)
                continue

            # ── Binary Black-Scholes fair value ───────────────────────────────
            fair_up = compute_fair(sol_price, threshold, minutes_left)
            fair_dn = round(1.0 - fair_up, 4)

            # ── Perp signal + divergence guard ────────────────────────────────
            perp_premium_pct, perp_signal = get_perp_signal(sol_price)
            market_up = market.get("market_up", 0.5)
            market_dn = market.get("market_dn", 0.5)
            div_skip_up, div_skip_dn, div_reason = check_market_divergence(
                fair_up, market_up, market_dn, perp_signal
            )

            # ── Close-out phase ───────────────────────────────────────────────
            if minutes_left < cfg.close_out_mins:
                log.warning(f"< {cfg.close_out_mins} min to close — entering close-out")
                for oid in list(active_orders.keys()):
                    cancel_order(oid)
                active_orders.clear()

                up_shares, dn_shares = get_inventory(up_outcome, dn_outcome)
                burnable = int(min(up_shares, dn_shares))
                if burnable > 0:
                    proceeds = burn_shares(market_id, burnable)
                    session_burn_recv += proceeds

                up_r = up_shares - burnable
                dn_r = dn_shares - burnable
                if up_r >= 0.5:
                    liquidate_residual(event_id, market_id, up_outcome,
                                       "UP", up_r, fair_up, min_notional, minutes_left)
                if dn_r >= 0.5:
                    liquidate_residual(event_id, market_id, dn_outcome,
                                       "DN", dn_r, fair_dn, min_notional, minutes_left)

                log_pnl(up_r, dn_r, fair_up, active_orders)
                log.info("Standing down until next 15min market...")
                time.sleep(max(60, int(minutes_left * 60) + 60))
                continue

            # ── Markup ────────────────────────────────────────────────────────
            markup, _ = get_markup(minutes_left)

            # ── Inventory + skewing ───────────────────────────────────────────
            up_shares, dn_shares = get_inventory(up_outcome, dn_outcome)
            up_bid_raw = round(max(cfg.min_bid_price, min(cfg.max_bid_price, fair_up - markup)), 3)
            dn_bid_raw = round(max(cfg.min_bid_price, min(cfg.max_bid_price, fair_dn - markup)), 3)
            up_bid_base, dn_bid_base = compute_skewed_bids(
                up_bid_raw, dn_bid_raw, up_shares, dn_shares
            )

            # ── Profit check: use the close-level (tightest) bid ──────────────
            # The close-level bid is the highest price we'll pay, so profit
            # is most constrained there. If there's no edge at close, skip all.
            implied_combined = (1.0 - dn_bid_base) + (1.0 - up_bid_base)
            net_per_pair     = round(implied_combined * (1.0 - fee_rate) - 1.0, 4)
            log.info(
                f"Fair: UP={fair_up:.4f} DN={fair_dn:.4f} | "
                f"BaseBid: UP={up_bid_base:.3f} DN={dn_bid_base:.3f} | "
                f"net/pair after fee=${net_per_pair:.4f}"
            )
            if net_per_pair <= 0:
                log.warning("No edge after markup + fee — skipping bids this cycle")
                time.sleep(cfg.reprice_interval_s)
                continue

            # ── Balance & exposure checks ─────────────────────────────────────
            balance = get_balance()
            log.info(f"Balance: ${balance:.4f}")
            if balance < cfg.min_balance_usd:
                log.warning(f"Balance below ${cfg.min_balance_usd} — pausing 60s")
                time.sleep(60)
                continue

            open_notional = sum(o.get("amount", cfg.bid_amount_usd) for o in active_orders.values())
            if open_notional >= cfg.session_max_notional:
                log.warning(f"Max open notional ${cfg.session_max_notional:.2f} reached — holding")
                log_pnl(up_shares, dn_shares, fair_up, active_orders)
                time.sleep(cfg.reprice_interval_s)
                continue

            # ── Reprice decision ──────────────────────────────────────────────
            fair_moved = (last_fair_up is None) or (abs(fair_up - last_fair_up) >= cfg.reprice_threshold)
            no_orders  = len(active_orders) == 0

            if fair_moved or no_orders:
                log.info(
                    f"Repricing | fair_up: {last_fair_up} → {fair_up} | "
                    f"active_orders: {len(active_orders)}"
                )
                for oid in list(active_orders.keys()):
                    cancel_order(oid)
                active_orders.clear()

                # ── Gate checks (adverse + divergence + circuit breaker) ───────
                place_up = True
                place_dn = True

                if fill_tracker.is_adverse():
                    dominant = fill_tracker.dominant_side()
                    log.warning(
                        f"Adverse selection: {dominant} fills at "
                        f"{fill_tracker.up_fill_rate():.0%} "
                        f"({fill_tracker.fill_count()} fills) — pausing {dominant} bid"
                    )
                    if dominant == "UP":
                        place_up = False
                    else:
                        place_dn = False

                if div_skip_up:
                    place_up = False
                    log.warning(f"SKIP BID UP (divergence guard: {div_reason})")
                if div_skip_dn:
                    place_dn = False
                    log.warning(f"SKIP BID DN (divergence guard: {div_reason})")

                up_exposure = up_shares * up_bid_base
                dn_exposure = dn_shares * dn_bid_base
                if up_exposure >= cfg.max_side_inventory_usd:
                    log.warning(
                        f"Circuit breaker: UP exposure ${up_exposure:.2f} "
                        f">= ${cfg.max_side_inventory_usd:.2f} — skipping UP bid"
                    )
                    place_up = False
                if dn_exposure >= cfg.max_side_inventory_usd:
                    log.warning(
                        f"Circuit breaker: DN exposure ${dn_exposure:.2f} "
                        f">= ${cfg.max_side_inventory_usd:.2f} — skipping DN bid"
                    )
                    place_dn = False

                # ── Place multi-level bids ─────────────────────────────────────
                if place_up:
                    new = place_multilevel_bids(
                        event_id, market_id, up_outcome,
                        "UP", up_bid_base, cfg.bid_amount_usd, min_notional,
                    )
                    active_orders.update(new)
                else:
                    log.info("SKIP ALL UP LEVELS (gate check active)")

                if place_dn:
                    new = place_multilevel_bids(
                        event_id, market_id, dn_outcome,
                        "DN", dn_bid_base, cfg.bid_amount_usd, min_notional,
                    )
                    active_orders.update(new)
                else:
                    log.info("SKIP ALL DN LEVELS (gate check active)")

                last_fair_up = fair_up
            else:
                log.info(f"Fair stable (Δ={abs(fair_up - last_fair_up):.4f}) — keeping orders")

            log.info(f"Active bid orders: {len(active_orders)} across levels")
            log.info(f"Perp signal: {perp_signal} ({perp_premium_pct:+.3f}%) | Div guard: {div_reason}")

            # ── Stop-loss check ───────────────────────────────────────────────
            net_pnl = compute_net_pnl(up_shares, dn_shares, fair_up)
            if check_stop_loss(net_pnl, active_orders, market):
                break

            log_pnl(up_shares, dn_shares, fair_up, active_orders)
            log.info(f"Sleeping {cfg.reprice_interval_s}s...")
            time.sleep(cfg.reprice_interval_s)

    except KeyboardInterrupt:
        log.info("Interrupted — cancelling bids and cleaning up inventory...")
        for oid in active_orders:
            cancel_order(oid)

        market = find_sol_market()
        if market:
            up_s, dn_s = get_inventory(market["up_outcome_id"], market["dn_outcome_id"])
            burnable   = int(min(up_s, dn_s))
            if burnable > 0:
                proceeds = burn_shares(market["market_id"], burnable)
                session_burn_recv += proceeds
            up_r, dn_r = up_s - burnable, dn_s - burnable
            mins   = get_minutes_to_close(market)
            fair_u = last_fair_up or 0.5
            if up_r >= 0.5:
                liquidate_residual(market["event_id"], market["market_id"],
                                   market["up_outcome_id"], "UP", up_r, fair_u,
                                   market["min_notional"], mins)
            if dn_r >= 0.5:
                liquidate_residual(market["event_id"], market["market_id"],
                                   market["dn_outcome_id"], "DN", dn_r, 1.0 - fair_u,
                                   market["min_notional"], mins)

        log_pnl(0, 0, last_fair_up or 0.5, {})

        if market_history:
            log.info(f"Session summary: {len(market_history)} markets completed")
            for m in market_history:
                log.info(f"  {m['market_id'][:8]} | pnl={m['net_pnl']:+.4f} | fill_rate={m['fill_rate']:.0%}")

        log.info("=== SOL MM v6 stopped ===")


if __name__ == "__main__":
    run()
