"""
╔══════════════════════════════════════════════════════════════════════════╗
║      BAYSE MARKETS — SOLANA AMM MARKET MAKER                             ║
║  Strategy: AMM (Constant Product) + Volatility Adjustment               ║
║  Engine: AMM (not CLOB) | Instrument: SOL Price Prediction              ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Core Logic:                                                             ║
║   1. Fetch live SOL price from multiple sources                          ║
║   2. Compute fair price using volatility-adjusted AMM formula           ║
║   3. Place two-sided quotes on AMM pool                                  ║
║   4. Dynamic spread based on volatility and inventory                   ║
║   5. Auto-rebalance when inventory imbalanced                           ║
╚══════════════════════════════════════════════════════════════════════════╝
"""
import os
import hashlib
import hmac
import json
import logging
import time
import statistics
from base64 import b64encode
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict
from collections import deque

import requests
from dotenv import load_dotenv

load_dotenv()

# ════════════════════════════════════════════════════════════════════════════
# CREDENTIALS
# ════════════════════════════════════════════════════════════════════════════
PUBLIC_KEY = os.getenv("PUBLIC_KEY", "")
SECRET_KEY = os.getenv("SECRET_KEY", "")
BASE_URL = os.getenv("BASE_URL", "https://relay.bayse.markets")

# ════════════════════════════════════════════════════════════════════════════
# AMM STRATEGY PARAMETERS
# ════════════════════════════════════════════════════════════════════════════

# ── AMM Core ────────────────────────────────────────────────────────────────
AMM_FEE = 0.003              # 0.3% fee per trade (typical for AMMs)
BASE_SPREAD = 0.02           # 2% base spread
VOLATILITY_SCALE = 1.5       # κ = 1.5 (spread multiplier for volatility)
INVENTORY_SCALE = 0.15       # γ = 0.15 (inventory adjustment)
MAX_INVENTORY = 30           # Max shares in either direction
DRAWDOWN_LIMIT = 10.0        # $10 max drawdown before pausing

# ── Price Feed ──────────────────────────────────────────────────────────────
PRICE_SOURCES = [
    "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT",
    "https://api.coinbase.com/v2/prices/SOL-USD/spot",
    "https://api.kraken.com/0/public/Ticker?pair=SOLUSD"
]
VOLATILITY_WINDOW = 20       # Minutes for volatility calculation

# ── Risk Management ─────────────────────────────────────────────────────────
MIN_BALANCE_USD = 10.0       # Minimum wallet balance
MAX_SPREAD = 0.15            # Max 15% spread (prevents extreme quotes)
REPRICE_INTERVAL = 3         # Seconds between quote updates
ORDER_SIZE_USD = 1.0         # $1 per side (small for testing)

# ── AMM Pool Parameters ─────────────────────────────────────────────────────
# For AMM engine, we quote both YES and NO continuously
# Reserves track our inventory in each outcome
INITIAL_RESERVES = 1000      # Starting liquidity in each pool

CURRENCY = "USD"


# ════════════════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SOL_AMM_MM")


# ════════════════════════════════════════════════════════════════════════════
# RATE LIMITER & CACHE (from Polymarket pattern)
# ════════════════════════════════════════════════════════════════════════════
class RateLimiter:
    def __init__(self):
        self.remaining = 100
        self.reset_at = 0
    
    def check(self):
        if self.remaining <= 0 and time.time() < self.reset_at:
            wait = self.reset_at - time.time()
            log.warning(f"Rate limited, sleeping {wait:.1f}s")
            time.sleep(wait)
    
    def update_from_headers(self, headers):
        if 'X-RateLimit-Remaining' in headers:
            self.remaining = int(headers['X-RateLimit-Remaining'])
        if 'X-RateLimit-Reset' in headers:
            self.reset_at = int(headers['X-RateLimit-Reset'])

rate_limiter = RateLimiter()


# ════════════════════════════════════════════════════════════════════════════
# API AUTHENTICATION (Bayse Markets)
# ════════════════════════════════════════════════════════════════════════════
def _write_headers(method: str, path: str, body: Optional[dict] = None) -> dict:
    """HMAC-SHA256 signed headers for Bayse Markets API."""
    ts = str(int(time.time()))
    body_str = json.dumps(body, separators=(",", ":")) if body else ""
    body_hash = hashlib.sha256(body_str.encode()).hexdigest() if body_str else ""
    payload = f"{ts}.{method.upper()}.{path}.{body_hash}"
    sig = b64encode(
        hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "X-Public-Key": PUBLIC_KEY,
        "X-Timestamp": ts,
        "X-Signature": sig,
        "Content-Type": "application/json",
    }


def _read_headers() -> dict:
    return {"X-Public-Key": PUBLIC_KEY}


def _get(path: str, params: dict = None) -> Optional[dict]:
    rate_limiter.check()
    try:
        r = requests.get(f"{BASE_URL}{path}", headers=_read_headers(), params=params, timeout=10)
        rate_limiter.update_from_headers(r.headers)
        if r.ok:
            return r.json()
        log.warning(f"GET {path} → {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"GET {path} error: {e}")
    return None


def _post(path: str, body: dict) -> Optional[dict]:
    rate_limiter.check()
    try:
        r = requests.post(
            f"{BASE_URL}{path}",
            data=json.dumps(body, separators=(",", ":")),
            headers=_write_headers("POST", path, body),
            timeout=10,
        )
        rate_limiter.update_from_headers(r.headers)
        if r.ok:
            return r.json()
        log.warning(f"POST {path} → {r.status_code}: {r.text[:300]}")
    except Exception as e:
        log.error(f"POST {path} error: {e}")
    return None


def _delete(path: str) -> bool:
    rate_limiter.check()
    try:
        r = requests.delete(f"{BASE_URL}{path}", headers=_write_headers("DELETE", path), timeout=10)
        rate_limiter.update_from_headers(r.headers)
        return r.status_code in (200, 204)
    except Exception as e:
        log.error(f"DELETE {path} error: {e}")
    return False


# ════════════════════════════════════════════════════════════════════════════
# SOL PRICE FEED (Multi-source with anomaly detection)
# ════════════════════════════════════════════════════════════════════════════
class SolanaPriceFeed:
    def __init__(self):
        self.price_history = deque(maxlen=VOLATILITY_WINDOW)
        self.last_price = 0.0
        self.last_update = 0
    
    def fetch_price(self) -> Optional[float]:
        """Fetch SOL price from multiple sources, return median."""
        prices = []
        
        for source in PRICE_SOURCES:
            try:
                r = requests.get(source, timeout=3)
                if r.status_code == 200:
                    data = r.json()
                    # Parse different API formats
                    if "binance" in source:
                        price = float(data["price"])
                    elif "coinbase" in source:
                        price = float(data["data"]["amount"])
                    elif "kraken" in source:
                        price = float(data["result"]["XXSOLZUSD"]["c"][0])
                    else:
                        continue
                    
                    # Basic sanity check (SOL between $10 and $1000)
                    if 10 < price < 1000:
                        prices.append(price)
            except Exception as e:
                log.debug(f"Price source failed {source}: {e}")
        
        if not prices:
            log.error("No valid price sources")
            return None
        
        # Use median to filter outliers
        price = statistics.median(prices)
        
        # Update history for volatility calculation
        self.price_history.append(price)
        self.last_price = price
        self.last_update = time.time()
        
        return price
    
    def get_volatility(self) -> float:
        """Calculate annualized volatility from price history."""
        if len(self.price_history) < 2:
            return 0.5  # Default 50% volatility
        
        # Calculate log returns
        prices = list(self.price_history)
        returns = [prices[i] / prices[i-1] - 1 for i in range(1, len(prices))]
        
        if not returns:
            return 0.5
        
        # Annualized volatility (sqrt(525600 minutes per year))
        std_returns = statistics.stdev(returns)
        annualized_vol = std_returns * (525600 / VOLATILITY_WINDOW) ** 0.5
        
        # Cap at reasonable range
        return min(2.0, max(0.1, annualized_vol))
    
    def get_price_change(self) -> float:
        """Get 1-minute price change percentage."""
        if len(self.price_history) < 2:
            return 0.0
        return (self.price_history[-1] - self.price_history[-2]) / self.price_history[-2]


# ════════════════════════════════════════════════════════════════════════════
# AMM MARKET DISCOVERY (Find SOL prediction market)
# ════════════════════════════════════════════════════════════════════════════
@dataclass
class MarketInfo:
    event_id: str
    market_id: str
    yes_outcome_id: str
    no_outcome_id: str
    title: str
    strike_price: float  # Target SOL price (e.g., $100)
    resolution_time: datetime
    engine: str  # "AMM" or "CLOB"


def find_sol_market() -> Optional[MarketInfo]:
    """Find active SOL price prediction market on Bayse."""
    data = _get("/v1/pm/events", {"page": 1, "size": 50})
    if not data:
        return None
    
    candidates = []
    for event in data.get("events", data.get("data", [])):
        title = event.get("title", "")
        # Look for SOL-related markets
        if not ("SOL" in title.upper() or "SOLANA" in title.upper()):
            continue
        if event.get("status", "").lower() != "open":
            continue
        
        for market in event.get("markets", []):
            # Support both AMM and CLOB engines
            engine = market.get("engine", "").upper()
            if engine not in ["AMM", "CLOB"]:
                continue
            
            # Parse resolution time
            closes_at_str = market.get("closesAt") or event.get("closesAt")
            if not closes_at_str:
                continue
            try:
                resolution_time = datetime.fromisoformat(closes_at_str.replace("Z", "+00:00"))
            except Exception:
                continue
            
            if resolution_time <= datetime.now(timezone.utc):
                continue
            
            # Extract strike price (e.g., "$100" from title)
            strike = _extract_strike_price(title)
            if strike is None:
                continue
            
            # Get outcome IDs
            outcomes = market.get("outcomes", [])
            yes_id = no_id = None
            for o in outcomes:
                label = str(o.get("label", o.get("outcome", ""))).upper()
                if label in ("YES", "TRUE", "ABOVE"):
                    yes_id = o.get("id")
                elif label in ("NO", "FALSE", "BELOW"):
                    no_id = o.get("id")
            
            if yes_id and no_id:
                candidates.append(MarketInfo(
                    event_id=event["id"],
                    market_id=market["id"],
                    yes_outcome_id=yes_id,
                    no_outcome_id=no_id,
                    title=title,
                    strike_price=strike,
                    resolution_time=resolution_time,
                    engine=engine,
                ))
    
    if not candidates:
        return None
    
    # Return the market with highest volume or soonest closing
    candidates.sort(key=lambda m: m.resolution_time)
    return candidates[0]


def _extract_strike_price(text: str) -> Optional[float]:
    """Extract SOL price target from title (e.g., 'above $100', '> $50')."""
    patterns = [
        r'\$(\d+(?:\.\d+)?)',           # $100, $50.50
        r'above\s+\$?(\d+(?:\.\d+)?)',  # above $100
        r'below\s+\$?(\d+(?:\.\d+)?)',  # below $50
        r'(\d+(?:\.\d+)?)\s*USD',       # 100 USD
        r'(\d+(?:\.\d+)?)\s*SOL',       # 100 SOL
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return float(match.group(1))
    return None


# ════════════════════════════════════════════════════════════════════════════
# AMM QUOTE ENGINE (Constant Product Formula)
# ════════════════════════════════════════════════════════════════════════════
@dataclass
class AMMState:
    """Tracks AMM pool state and inventory."""
    yes_reserves: float = INITIAL_RESERVES
    no_reserves: float = INITIAL_RESERVES
    inventory_yes: float = 0.0  # Positive = long YES
    inventory_no: float = 0.0    # Positive = long NO
    total_pnl: float = 0.0
    peak_pnl: float = 0.0
    drawdown: float = 0.0


class AMMQuoteEngine:
    """AMM-based quoting with volatility and inventory adjustments."""
    
    def __init__(self, market_info: MarketInfo, price_feed: SolanaPriceFeed):
        self.market = market_info
        self.price_feed = price_feed
        self.state = AMMState()
        self.last_quote_time = 0
        self.active_orders = {}
        
    def calculate_fair_probability(self, current_price: float) -> float:
        """
        Calculate fair probability based on distance to strike.
        Uses logistic function for smoother probability curve.
        """
        distance = current_price - self.market.strike_price
        # Volatility-adjusted sensitivity
        vol = self.price_feed.get_volatility()
        # Wider distance = more extreme probability
        # Using sigmoid: P = 1 / (1 + exp(-distance * sensitivity))
        sensitivity = 0.1 / max(vol, 0.1)  # Higher volatility = lower sensitivity
        fair_prob = 1.0 / (1.0 + math.exp(-distance * sensitivity))
        
        # Clamp to reasonable bounds
        return max(0.05, min(0.95, fair_prob))
    
    def calculate_amm_price(self, fair_prob: float, inventory_imbalance: float) -> Tuple[float, float]:
        """
        Calculate bid/ask prices using AMM formula with adjustments.
        
        Base formula: P = fair_prob * (1 + γ * inventory)
        Spread = κ * σ (volatility scaling)
        
        Returns (bid_price, ask_price) for YES outcome
        """
        # Get current volatility
        volatility = self.price_feed.get_volatility()
        
        # Base spread from volatility (κ * σ)
        base_spread = VOLATILITY_SCALE * volatility
        base_spread = min(MAX_SPREAD, max(BASE_SPREAD, base_spread))
        
        # Inventory adjustment (γ * inventory)
        # Positive inventory (long YES) means we want to sell cheaper
        inventory_adjustment = INVENTORY_SCALE * inventory_imbalance
        inventory_adjustment = max(-base_spread / 2, min(base_spread / 2, inventory_adjustment))
        
        # Mid price (our fair probability)
        mid_price = fair_prob
        
        # Calculate bid/ask with spread
        half_spread = base_spread / 2
        bid_price = max(0.01, mid_price - half_spread - inventory_adjustment)
        ask_price = min(0.99, mid_price + half_spread - inventory_adjustment)
        
        # Ensure bid < ask and no arbitrage
        if bid_price >= ask_price:
            bid_price = max(0.01, mid_price - 0.01)
            ask_price = min(0.99, mid_price + 0.01)
        
        return bid_price, ask_price
    
    def get_inventory_imbalance(self) -> float:
        """Calculate net inventory position (positive = long YES)."""
        # Normalize by max inventory
        net_position = (self.state.inventory_yes - self.state.inventory_no) / MAX_INVENTORY
        return max(-1.0, min(1.0, net_position))
    
    def should_quote(self) -> bool:
        """Check if we should continue quoting."""
        # Check drawdown limit
        if self.state.drawdown > DRAWDOWN_LIMIT:
            log.warning(f"Drawdown limit reached: ${self.state.drawdown:.2f}")
            return False
        
        # Check if market is still open
        if datetime.now(timezone.utc) > self.market.resolution_time:
            return False
        
        return True
    
    def update_pnl(self, trade_side: str, trade_price: float, trade_size: float):
        """Update P&L tracking after each trade."""
        # Simplified P&L: track average entry vs current fair value
        if trade_side == "BUY_YES":
            self.state.inventory_yes += trade_size
        elif trade_side == "SELL_YES":
            self.state.inventory_yes -= trade_size
        elif trade_side == "BUY_NO":
            self.state.inventory_no += trade_size
        elif trade_side == "SELL_NO":
            self.state.inventory_no -= trade_size
        
        # Update P&L (mark to market)
        current_price = self.price_feed.last_price
        fair_prob = self.calculate_fair_probability(current_price)
        
        # Unrealized P&L = inventory * (current_prob - entry_prob_avg)
        # This is simplified; real P&L would track each trade
        self.state.total_pnl = (self.state.inventory_yes * fair_prob + 
                                self.state.inventory_no * (1 - fair_prob))
        
        # Track drawdown
        if self.state.total_pnl > self.state.peak_pnl:
            self.state.peak_pnl = self.state.total_pnl
        self.state.drawdown = self.state.peak_pnl - self.state.total_pnl


# ════════════════════════════════════════════════════════════════════════════
# ORDER MANAGEMENT (AMM-specific)
# ════════════════════════════════════════════════════════════════════════════
def place_amm_order(
    market_info: MarketInfo,
    outcome_id: str,
    side: str,  # "BUY" or "SELL"
    price: float,
    amount_usd: float,
) -> Optional[str]:
    """
    Place order on AMM engine.
    For AMM, we use the same order endpoint but with engine=AMM.
    """
    path = f"/v1/pm/events/{market_info.event_id}/markets/{market_info.market_id}/orders"
    body = {
        "outcomeId": outcome_id,
        "side": side,
        "type": "LIMIT",
        "price": round(price, 3),
        "amount": round(amount_usd, 2),
        "currency": CURRENCY,
        "timeInForce": "GTC",
    }
    
    data = _post(path, body)
    if data:
        order_id = data.get("order", {}).get("id")
        if order_id:
            log.info(f"  📋 {side} @ {price:.3f} (${amount_usd:.2f}) → {order_id[:12]}…")
            return order_id
        else:
            log.warning(f"  ⚠️ Order response missing ID: {data}")
    return None


def cancel_amm_order(order_id: str) -> bool:
    """Cancel order on AMM engine."""
    return _delete(f"/v1/pm/orders/{order_id}")


# ════════════════════════════════════════════════════════════════════════════
# PORTFOLIO & RISK MANAGEMENT
# ════════════════════════════════════════════════════════════════════════════
def get_portfolio_balance() -> float:
    """Get USD balance."""
    data = _get("/v1/wallet/assets")
    if not data:
        return 0.0
    assets = data.get("assets", data if isinstance(data, list) else [])
    for asset in assets:
        if asset.get("currency", "").upper() == "USD":
            return float(asset.get("availableBalance", asset.get("balance", 0)))
    return 0.0


def sync_inventory(market_info: MarketInfo, amm_engine: AMMQuoteEngine):
    """Sync inventory from API to local state."""
    data = _get("/v1/pm/portfolio")
    if not data:
        return
    
    yes_bal = no_bal = 0.0
    for pos in data.get("outcomeBalances", []):
        mkt = pos.get("market", {})
        if mkt.get("id") != market_info.market_id:
            continue
        outcome = pos.get("outcome", "").upper()
        bal = float(pos.get("availableBalance", pos.get("balance", 0)))
        if outcome == "YES":
            yes_bal += bal
        elif outcome == "NO":
            no_bal += bal
    
    amm_engine.state.inventory_yes = yes_bal
    amm_engine.state.inventory_no = no_bal
    log.debug(f"Synced inventory: YES={yes_bal:.1f}, NO={no_bal:.1f}")


# ════════════════════════════════════════════════════════════════════════════
# MAIN QUOTING LOOP
# ════════════════════════════════════════════════════════════════════════════
def run_quoting_loop(market_info: MarketInfo):
    """Main AMM market making loop."""
    
    # Initialize components
    price_feed = SolanaPriceFeed()
    amm_engine = AMMQuoteEngine(market_info, price_feed)
    
    log.info(f"🔌 Connecting to SOL price feed…")
    log.info(f"🔌 Connecting to market data feed…")
    log.info(f"🚀 Quoting loop started.")
    
    last_quote_time = 0
    failed_orders = 0
    max_failures = 10
    
    while amm_engine.should_quote():
        try:
            current_time = time.time()
            
            # Rate limit quoting
            if current_time - last_quote_time < REPRICE_INTERVAL:
                time.sleep(0.1)
                continue
            
            # 1. Fetch SOL price
            sol_price = price_feed.fetch_price()
            if sol_price is None:
                log.warning("Waiting for SOL price…")
                time.sleep(1)
                continue
            
            # 2. Calculate fair probability
            fair_prob = amm_engine.calculate_fair_probability(sol_price)
            
            # 3. Get inventory imbalance
            inventory_imb = amm_engine.get_inventory_imbalance()
            
            # 4. Calculate AMM quotes
            bid_price, ask_price = amm_engine.calculate_amm_price(fair_prob, inventory_imb)
            
            # 5. Get volatility for display
            volatility = price_feed.get_volatility()
            
            # 6. Log current state
            log.info(
                f"💹 Quote  BID={bid_price:.3f}  ASK={ask_price:.3f}  "
                f"mid={(bid_price+ask_price)/2:.3f}  fair={fair_prob:.3f}  "
                f"σ={volatility:.4f}  inv={inventory_imb:+.1f}  SOL=${sol_price:.2f}"
            )
            
            # 7. Check balance
            balance = get_portfolio_balance()
            if balance < MIN_BALANCE_USD:
                log.warning(f"Low balance: ${balance:.2f} < ${MIN_BALANCE_USD}")
                time.sleep(10)
                continue
            
            # 8. Cancel stale orders
            for order_id in list(amm_engine.active_orders.keys()):
                cancel_amm_order(order_id)
            amm_engine.active_orders.clear()
            
            # 9. Place new orders (BUY and SELL on AMM)
            # Place BUY YES order at bid price
            order_id = place_amm_order(
                market_info,
                market_info.yes_outcome_id,
                "BUY",
                bid_price,
                ORDER_SIZE_USD
            )
            if order_id:
                amm_engine.active_orders[order_id] = {"side": "BUY_YES", "price": bid_price}
            else:
                failed_orders += 1
            
            # Place SELL YES order at ask price
            order_id = place_amm_order(
                market_info,
                market_info.yes_outcome_id,
                "SELL",
                ask_price,
                ORDER_SIZE_USD
            )
            if order_id:
                amm_engine.active_orders[order_id] = {"side": "SELL_YES", "price": ask_price}
            else:
                failed_orders += 1
            
            # Also quote NO side (complementary)
            no_bid = 1 - ask_price
            no_ask = 1 - bid_price
            
            order_id = place_amm_order(
                market_info,
                market_info.no_outcome_id,
                "BUY",
                no_bid,
                ORDER_SIZE_USD
            )
            if order_id:
                amm_engine.active_orders[order_id] = {"side": "BUY_NO", "price": no_bid}
            else:
                failed_orders += 1
            
            order_id = place_amm_order(
                market_info,
                market_info.no_outcome_id,
                "SELL",
                no_ask,
                ORDER_SIZE_USD
            )
            if order_id:
                amm_engine.active_orders[order_id] = {"side": "SELL_NO", "price": no_ask}
            else:
                failed_orders += 1
            
            # 10. Check for failures
            if failed_orders >= max_failures:
                log.error(f"Too many order failures ({failed_orders}), pausing...")
                time.sleep(30)
                failed_orders = 0
            
            # 11. Sync inventory periodically
            if int(current_time) % 30 < 2:  # Every ~30 seconds
                sync_inventory(market_info, amm_engine)
            
            last_quote_time = current_time
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.error(f"Quote loop error: {e}", exc_info=True)
            time.sleep(5)
    
    # Close out: Cancel all orders
    log.info("Closing out - cancelling all orders...")
    for order_id in list(amm_engine.active_orders.keys()):
        cancel_amm_order(order_id)
    
    log.info(f"Final P&L: ${amm_engine.state.total_pnl:.2f}")


# ════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════
def main():
    log.info("═" * 62)
    log.info("  BAYSE MARKETS — SOLANA AMM MARKET MAKER")
    log.info("═" * 62)
    log.info(f"  Strategy:  AMM (Constant Product)")
    log.info(f"  Spread:    κ={VOLATILITY_SCALE} | γ={INVENTORY_SCALE}")
    log.info(f"  Risk:      drawdown=${DRAWDOWN_LIMIT} | inv_cap={MAX_INVENTORY} shares")
    log.info("═" * 62)
    
    if not PUBLIC_KEY or not SECRET_KEY:
        log.error("❌ BAYSE_PUBLIC_KEY and BAYSE_SECRET_KEY must be set in .env")
        return
    
    while True:
        try:
            # Find SOL market
            market = find_sol_market()
            if market is None:
                log.info("🔍 No active SOL market found — waiting 30s…")
                time.sleep(30)
                continue
            
            log.info(f"\n🟢 Found market: {market.title}")
            log.info(f"   Strike: ${market.strike_price} | Engine: {market.engine}")
            log.info(f"   Event ID: {market.event_id}")
            log.info(f"   Market ID: {market.market_id}")
            
            # Run quoting loop
            run_quoting_loop(market)
            
            # Market finished, wait for next
            log.info("⏳ Market closed, waiting 60s for next...")
            time.sleep(60)
            
        except KeyboardInterrupt:
            log.info("\n⛔ Shutting down market maker.")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(15)


if __name__ == "__main__":
    import re  # Add missing import
    main()
