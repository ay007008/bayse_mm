"""
Bayse Markets — EURGBP FX Hourly Market Making Engine
======================================================

Strategy:
  1. On startup, resolve the current open eurgbp-fx-hourly event & market IDs
  2. Subscribe to WebSocket order book + price updates for real-time feeds
  3. Every QUOTE_INTERVAL seconds: compute fair-value from mid-price + skew,
     cancel stale quotes and post fresh two-sided LIMIT orders (YES bid + ask)
  4. Track inventory (net YES position) and apply a skew to stay delta-neutral
  5. Respect liquidity-reward constraints (maxSpreadCents, minNotionalOrderSize)
  6. Hard-stop if balance falls below MIN_BALANCE_USD

Configuration is entirely via environment variables — see .env.example
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import signal
import base64
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import websockets
from dotenv import load_dotenv



load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ─── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("bayse-mm")


# ─── CONFIG ──────────────────────────────────────────────────────────────────
@dataclass
class Config:
    base_url: str            = os.getenv("BASE_URL", "https://relay.bayse.markets")
    ws_url: str              = os.getenv("BAYSE_WS_URL", "wss://socket.bayse.markets/ws/v1/markets")
    public_key: str          = os.getenv("PUBLIC_KEY", "")
    secret_key: str          = os.getenv("SECRET_KEY", "")
    currency: str            = os.getenv("BAYSE_CURRENCY", "USD")
    series_slug: str         = os.getenv("BAYSE_SERIES", "fx-eurgbp-1h")

    # Quoting
    quote_interval: float    = float(os.getenv("QUOTE_INTERVAL_MS", "15000")) / 1000
    order_size_usd: float    = float(os.getenv("ORDER_SIZE_USD", "5"))
    spread_cents: float      = float(os.getenv("SPREAD_CENTS", "3"))
    max_spread_cents: float  = float(os.getenv("MAX_SPREAD_CENTS", "8"))
    inventory_skew: float    = float(os.getenv("INVENTORY_SKEW_FACTOR", "0.005"))
    max_inventory: float     = float(os.getenv("MAX_INVENTORY_SHARES", "200"))
    min_balance: float       = float(os.getenv("MIN_BALANCE_USD", "20"))
    max_price: float         = 0.97
    min_price: float         = 0.03


# ─── STATE ───────────────────────────────────────────────────────────────────
@dataclass
class MarketState:
    event_id: Optional[str]    = None
    market_id: Optional[str]   = None
    outcome1_id: Optional[str] = None   # YES
    outcome2_id: Optional[str] = None   # NO
    mid_price: float           = 0.5
    best_bid: Optional[float]  = None
    best_ask: Optional[float]  = None
    balance: Optional[float]   = None
    inv_yes: float             = 0.0
    inv_no: float              = 0.0
    bid_order_id: Optional[str] = None
    ask_order_id: Optional[str] = None
    running: bool              = False


# ─── AUTH ────────────────────────────────────────────────────────────────────
def build_signature(cfg: Config, method: str, path: str, body: str = "") -> dict:
    timestamp = str(int(time.time()))
    body_hash = hashlib.sha256(body.encode()).hexdigest() if body else ""
    payload   = f"{timestamp}.{method}.{path}.{body_hash}"
    signature = base64.b64encode(
        hmac.new(cfg.secret_key.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "Content-Type": "application/json",
        "X-Public-Key":  cfg.public_key,
        "X-Timestamp":   timestamp,
        "X-Signature":   signature,
    }


def read_headers(cfg: Config) -> dict:
    return {"X-Public-Key": cfg.public_key}


# ─── HTTP CLIENT ─────────────────────────────────────────────────────────────
class BayseClient:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg     = cfg
        self.session = session

    async def _request(self, method: str, path: str, body: dict = None):
        url      = self.cfg.base_url + path
        body_str = json.dumps(body) if body else None
        is_write = method in ("POST", "DELETE", "PATCH", "PUT")
        headers  = (
            build_signature(self.cfg, method, path, body_str or "")
            if is_write
            else read_headers(self.cfg)
        )
        async with self.session.request(
            method, url, headers=headers,
            data=body_str.encode() if body_str else None
        ) as resp:
            text = await resp.text()
            if not resp.ok:
                raise RuntimeError(f"{method} {path} → {resp.status}: {text}")
            return json.loads(text) if text.strip() else None

    async def get(self, path: str):
        return await self._request("GET", path)

    async def post(self, path: str, body: dict):
        return await self._request("POST", path, body)

    async def delete(self, path: str):
        return await self._request("DELETE", path)


# ─── MARKET RESOLUTION ───────────────────────────────────────────────────────
async def resolve_event(client: BayseClient, cfg: Config, state: MarketState):
    log.info("Searching for open %s event …", cfg.series_slug)

    data = await client.get(
        f"/v1/pm/events?seriesSlug={cfg.series_slug}&status=open"
        f"&currency={cfg.currency}&size=5"
    )
    event = (data.get("events") or [None])[0] if data else None

    if not event:
        # Fallback: keyword search
        kw = await client.get(
            f"/v1/pm/events?keyword=eurgbp&status=open&currency={cfg.currency}&size=10"
        )
        for e in (kw or {}).get("events", []):
            s = (e.get("seriesSlug") or "") + (e.get("slug") or "")
            if "eurgbp" in s.lower():
                event = e
                break

    if not event:
        raise RuntimeError("No open EURGBP-FX-hourly event found.")

    markets = event.get("markets") or []
    if not markets:
        raise RuntimeError("Event has no markets.")

    market = markets[0]
    engine = event.get("engine", "").upper()
    if engine != "CLOB":
        raise RuntimeError(f"Market engine is {engine}. Market making requires CLOB.")

    state.event_id    = event["id"]
    state.market_id   = market["id"]
    state.outcome1_id = market["outcome1Id"]   # YES
    state.outcome2_id = market["outcome2Id"]   # NO
    state.mid_price   = market.get("outcome1Price", 0.5)

    log.info("✔ Event  : %s  (%s)", event.get("slug"), state.event_id)
    log.info("  Market : %s  (%s)", market.get("title"), state.market_id)
    log.info("  Engine : %s", engine)
    log.info("  YES id : %s  price=%.4f", state.outcome1_id, state.mid_price)
    log.info("  NO  id : %s  price=%.4f", state.outcome2_id, market.get("outcome2Price", 0))
    log.info("  Closes : %s", event.get("closingDate"))


# ─── PORTFOLIO ───────────────────────────────────────────────────────────────
async def refresh_portfolio(client: BayseClient, cfg: Config, state: MarketState):
    portfolio, assets = await asyncio.gather(
        client.get("/v1/pm/portfolio"),
        client.get("/v1/wallet/assets"),
    )

    usd = next(
        (a for a in (assets or {}).get("assets", []) if a.get("currency") == cfg.currency),
        None,
    )
    state.balance = usd.get("availableBalance") or usd.get("balance") if usd else None

    pos = next(
        (p for p in (portfolio or {}).get("positions", []) if p.get("marketId") == state.market_id),
        None,
    )
    if pos:
        state.inv_yes = pos.get("yesShares") or pos.get("outcome1Shares") or 0
        state.inv_no  = pos.get("noShares")  or pos.get("outcome2Shares") or 0

    log.info(
        "Balance: $%.2f | YES=%.1f  NO=%.1f",
        state.balance or 0, state.inv_yes, state.inv_no,
    )


# ─── OPEN ORDERS ─────────────────────────────────────────────────────────────
async def refresh_open_orders(client: BayseClient, state: MarketState):
    data = await client.get(
        f"/v1/pm/orders?marketId={state.market_id}&status=open&size=50"
    )
    state.bid_order_id = None
    state.ask_order_id = None
    for o in (data or {}).get("orders", []):
        if (o.get("outcome") or "").upper() != "YES":
            continue
        if o.get("side") == "BUY"  and not state.bid_order_id:
            state.bid_order_id = o["id"]
        if o.get("side") == "SELL" and not state.ask_order_id:
            state.ask_order_id = o["id"]


# ─── CANCEL ──────────────────────────────────────────────────────────────────
async def cancel_order(client: BayseClient, order_id: str, label: str):
    try:
        await client.delete(f"/v1/pm/orders/{order_id}")
        log.info("  Cancelled %s  id=%s", label, order_id)
    except Exception as e:
        log.warning("  Cancel %s failed: %s", label, e)


async def cancel_all_quotes(client: BayseClient, state: MarketState):
    tasks = []
    if state.bid_order_id:
        tasks.append(cancel_order(client, state.bid_order_id, "yesBid"))
    if state.ask_order_id:
        tasks.append(cancel_order(client, state.ask_order_id, "yesAsk"))
    if tasks:
        await asyncio.gather(*tasks)
    state.bid_order_id = None
    state.ask_order_id = None


# ─── PRICE LOGIC ─────────────────────────────────────────────────────────────
def compute_quotes(cfg: Config, state: MarketState) -> tuple[float, float]:
    mid         = state.mid_price
    half_spread = cfg.spread_cents / 100
    net_inv     = state.inv_yes - state.inv_no
    skew        = net_inv * cfg.inventory_skew

    bid = round(max(cfg.min_price, mid - half_spread - skew), 4)
    ask = round(min(cfg.max_price, mid + half_spread - skew), 4)

    # Minimum 1-cent spread
    if ask - bid < 0.01:
        ask = round(bid + 0.01, 4)

    # Clamp to liquidity-reward max spread
    reward_max = cfg.max_spread_cents / 100
    if ask - bid > reward_max:
        centre = (bid + ask) / 2
        bid = round(centre - reward_max / 2, 4)
        ask = round(centre + reward_max / 2, 4)

    return bid, ask


# ─── PLACE ORDER ─────────────────────────────────────────────────────────────
async def place_order(
    client: BayseClient,
    cfg: Config,
    state: MarketState,
    side: str,
    price: float,
    label: str,
) -> Optional[str]:
    body = {
        "side":        side,
        "outcomeId":   state.outcome1_id,
        "amount":      cfg.order_size_usd,
        "type":        "LIMIT",
        "currency":    cfg.currency,
        "price":       price,
        "timeInForce": "GTC",
        "postOnly":    True,
    }
    try:
        res   = await client.post(
            f"/v1/pm/events/{state.event_id}/markets/{state.market_id}/orders", body
        )
        order = (res or {}).get("order", {})
        log.info(
            "  Placed %-8s @ %.4f  id=%s  status=%s",
            label, price, order.get("id"), order.get("status"),
        )
        return order.get("id")
    except Exception as e:
        log.warning("  Place %s failed: %s", label, e)
        return None


# ─── QUOTE CYCLE ─────────────────────────────────────────────────────────────
async def run_quote_cycle(client: BayseClient, cfg: Config, state: MarketState):
    if not state.running:
        return

    try:
        await refresh_portfolio(client, cfg, state)

        # Kill-switch
        if state.balance is not None and state.balance < cfg.min_balance:
            log.warning(
                "Balance $%.2f below minimum $%.2f — shutting down.",
                state.balance, cfg.min_balance,
            )
            state.running = False
            return

        bid_price, ask_price = compute_quotes(cfg, state)
        spread = round(ask_price - bid_price, 4)
        log.info(
            "Quote cycle — mid=%.4f  bid=%.4f  ask=%.4f  spread=%.4f",
            state.mid_price, bid_price, ask_price, spread,
        )

        await refresh_open_orders(client, state)
        await cancel_all_quotes(client, state)

        tasks = []
        if state.inv_yes <= cfg.max_inventory:
            tasks.append(("BUY",  bid_price, "yesBid"))
        else:
            log.warning("YES inventory %.1f exceeds max — skipping bid.", state.inv_yes)

        if state.inv_no <= cfg.max_inventory:
            tasks.append(("SELL", ask_price, "yesAsk"))
        else:
            log.warning("NO inventory %.1f exceeds max — skipping ask.", state.inv_no)

        results = await asyncio.gather(
            *[place_order(client, cfg, state, s, p, l) for s, p, l in tasks]
        )

        # Map results back
        for i, (side, _, label) in enumerate(tasks):
            oid = results[i]
            if label == "yesBid":
                state.bid_order_id = oid
            else:
                state.ask_order_id = oid

    except Exception as e:
        log.error("Quote cycle error: %s", e)


# ─── WEBSOCKET ───────────────────────────────────────────────────────────────
async def ws_listener(cfg: Config, state: MarketState):
    while state.running:
        try:
            async with websockets.connect(cfg.ws_url) as ws:
                log.info("WebSocket connected")

                await ws.send(json.dumps({
                    "type":    "subscribe",
                    "channel": "prices",
                    "eventId": state.event_id,
                }))
                await ws.send(json.dumps({
                    "type":      "subscribe",
                    "channel":   "orderbook",
                    "marketIds": [state.market_id],
                    "currency":  cfg.currency,
                }))
                log.info("Subscribed to prices + orderbook")

                async for raw in ws:
                    for line in raw.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            msg = json.loads(line)
                            handle_ws_message(msg, state)
                        except Exception:
                            pass

        except Exception as e:
            if state.running:
                log.warning("WebSocket error: %s — reconnecting in 5 s …", e)
                await asyncio.sleep(5)


def handle_ws_message(msg: dict, state: MarketState):
    mtype = msg.get("type")

    if mtype == "price_update":
        for m in msg.get("data", {}).get("markets", []):
            if m.get("id") == state.market_id:
                yes_price = (m.get("prices") or {}).get("YES")
                if yes_price is not None:
                    state.mid_price = yes_price

    elif mtype == "orderbook_update":
        ob = msg.get("data", {}).get("orderbook", {})
        if ob.get("marketId") != state.market_id:
            return
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        state.best_bid = bids[0]["price"] if bids else None
        state.best_ask = asks[0]["price"] if asks else None
        if state.best_bid and state.best_ask:
            state.mid_price = round((state.best_bid + state.best_ask) / 2, 4)


# ─── QUOTE LOOP ──────────────────────────────────────────────────────────────
async def quote_loop(client: BayseClient, cfg: Config, state: MarketState):
    while state.running:
        await run_quote_cycle(client, cfg, state)
        await asyncio.sleep(cfg.quote_interval)


# ─── SHUTDOWN ────────────────────────────────────────────────────────────────
async def shutdown(client: BayseClient, state: MarketState):
    state.running = False
    log.info("Shutting down — cancelling all open quotes …")
    try:
        await refresh_open_orders(client, state)
        await cancel_all_quotes(client, state)
    except Exception as e:
        log.warning("Cleanup error: %s", e)
    log.info("Done.")


# ─── MAIN ────────────────────────────────────────────────────────────────────
async def main():
    log.info("═══ Bayse Markets — EURGBP FX Hourly Market Maker ═══")

    cfg = Config()

    if not cfg.public_key or not cfg.secret_key:
        log.error("BAYSE_PUBLIC_KEY and BAYSE_SECRET_KEY must be set.")
        raise SystemExit(1)

    state = MarketState()

    async with aiohttp.ClientSession() as session:
        client = BayseClient(cfg, session)

        await resolve_event(client, cfg, state)
        await refresh_portfolio(client, cfg, state)

        state.running = True

        # Graceful shutdown on SIGINT / SIGTERM
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(shutdown(client, state))
            )

        # Run WebSocket listener and quote loop concurrently
        await asyncio.gather(
            ws_listener(cfg, state),
            quote_loop(client, cfg, state),
        )


if __name__ == "__main__":
    asyncio.run(main())
