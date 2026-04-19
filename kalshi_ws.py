"""Kalshi WebSocket client that keeps LIVE_PRICES up-to-date.

Runs as an asyncio background task inside the FastAPI process. It
connects to Kalshi's v2 WebSocket feed, subscribes to the "ticker"
channel for every market ticker we know about (refreshed as the REST
snapshot rebuilds), and writes bid/ask updates into the module-level
LIVE_PRICES dict.

main.py reads LIVE_PRICES on every /api/events response to overlay
fresh prices on top of the cached snapshot, so chances and YES/NO
prices stay near real time without a full re-fetch from Kalshi REST.
"""
import asyncio
import base64
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

try:
    import websockets
except ImportError:
    websockets = None

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except ImportError:
    serialization = None
    padding = None
    hashes = None

log = logging.getLogger("kalshi_ws")

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"
SUB_BATCH_SIZE = 500  # market tickers per subscribe message

# Module-level live price store. Keys are market tickers, values are
# dicts containing yes_bid, yes_ask, no_bid, no_ask (all in cents as
# floats) plus ts_ms of the last update we saw.
LIVE_PRICES: dict = {}

# Buffer for price snapshots that get flushed to the database
# every PRICE_FLUSH_INTERVAL seconds. Keeps the WS message loop
# fast (just appends to a list) while writing to DB in batches.
_price_buffer: list = []
PRICE_FLUSH_INTERVAL = 10  # seconds between DB flushes

# Status info exposed via /api/ws_status for debugging.
STATUS = {
    "connected": False,
    "connected_since": None,
    "subscribed": 0,
    "messages": 0,
    "updates": 0,
    "last_error": None,
}

# Ring buffer of the last N raw messages we received, for debugging the
# Kalshi WS message schema. Exposed via /api/ws_raw.
RAW_SAMPLES = deque(maxlen=30)


# ── Browser pub/sub ─────────────────────────────────────────────────
# Each browser WebSocket client registers its own asyncio.Queue here
# with the set of tickers it cares about. When a Kalshi price update
# arrives, we fan-out the delta to every queue whose ticker set
# overlaps. The main.py /ws/prices endpoint owns the queue lifetime —
# it creates one on connect, subscribes to tickers, and drops it on
# disconnect.
class BrowserSubscriber:
    def __init__(self, tickers: set = None):
        self.tickers: set = tickers or set()
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=500)

    def subscribe(self, tickers):
        self.tickers.update(tickers)

    def unsubscribe(self, tickers):
        self.tickers.difference_update(tickers)


_browser_subscribers: set = set()


def register_browser(sub: "BrowserSubscriber"):
    _browser_subscribers.add(sub)


def unregister_browser(sub: "BrowserSubscriber"):
    _browser_subscribers.discard(sub)


def _broadcast_to_browsers(ticker: str, update: dict):
    """Non-blocking fan-out of a single-ticker update to every browser
    subscriber that cares about this ticker. Drops the message on any
    subscriber whose queue is full (slow consumer protection)."""
    if not _browser_subscribers:
        return
    payload = {"type": "price", "ticker": ticker, "data": update}
    for sub in list(_browser_subscribers):
        if ticker not in sub.tickers:
            continue
        try:
            sub.queue.put_nowait(payload)
        except asyncio.QueueFull:
            # Slow consumer — drop this tick. Client will pick up on
            # the next update (or resync via REST if badly behind).
            pass


def _load_private_key():
    key_str = os.environ.get("KALSHI_PRIVATE_KEY")
    if not key_str or serialization is None:
        return None
    try:
        return serialization.load_pem_private_key(key_str.encode(), password=None)
    except Exception as e:
        log.error("failed to load private key: %s", e)
        return None


def _sign_headers(private_key):
    """Build Kalshi's signed auth headers for the WS upgrade handshake."""
    ts_ms = str(int(time.time() * 1000))
    msg = (ts_ms + "GET" + WS_PATH).encode()
    sig = private_key.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": os.environ["KALSHI_API_KEY_ID"],
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def _extract_update(msg):
    """Parse one incoming WS message and return (ticker, update_dict) or
    None if it's not a price update we care about. The ticker channel
    sends *_dollars string fields (e.g. "0.17") which we convert to
    cents; we also accept raw cents fields as a fallback in case Kalshi
    ever sends that shape."""
    if not isinstance(msg, dict):
        return None
    # Ignore explicit error frames (e.g. "Unknown channel name").
    if msg.get("type") == "error":
        return None
    body = msg.get("msg") or msg.get("data") or msg
    if not isinstance(body, dict):
        return None
    tk = body.get("market_ticker") or body.get("ticker")
    if not tk:
        return None
    fields = {}
    # Dollar-string fields (what Kalshi's "ticker" channel actually sends).
    # price_dollars is the most-recent trade price, which main.py
    # prefers as the "chance" display for markets that have traded.
    for k_dollars, k_out in (
        ("yes_bid_dollars", "yes_bid"),
        ("yes_ask_dollars", "yes_ask"),
        ("no_bid_dollars",  "no_bid"),
        ("no_ask_dollars",  "no_ask"),
        ("price_dollars",   "last_price"),
    ):
        v = body.get(k_dollars)
        if v is not None:
            try:
                fields[k_out] = float(v) * 100
            except Exception:
                pass
    # Raw cents fields as a fallback.
    for k in ("yes_bid", "yes_ask", "no_bid", "no_ask"):
        if k in fields:
            continue
        v = body.get(k)
        if v is not None:
            try:
                fields[k] = float(v)
            except Exception:
                pass
    if not fields:
        return None
    # The Kalshi "ticker" channel only sends yes-side prices. Derive
    # the NO side so live updates stay consistent on both boxes:
    #   no_ask = 100 - yes_bid   (best NO ask mirrors best YES bid)
    #   no_bid = 100 - yes_ask   (best NO bid mirrors best YES ask)
    if "yes_bid" in fields and "no_ask" not in fields:
        fields["no_ask"] = 100 - fields["yes_bid"]
    if "yes_ask" in fields and "no_bid" not in fields:
        fields["no_bid"] = 100 - fields["yes_ask"]
    # Volume & open interest — Kalshi's ticker channel sends
    # cumulative totals (not deltas). Field names vary by channel
    # version; try the common aliases.
    for src_key, out_key in (
        ("volume",             "volume"),
        ("total_volume",       "volume"),
        ("dollar_volume",      "dollar_volume"),
        ("open_interest",      "open_interest"),
        ("total_open_interest","open_interest"),
        ("dollar_open_interest","dollar_open_interest"),
    ):
        v = body.get(src_key)
        if v is not None and out_key not in fields:
            try:
                fields[out_key] = float(v)
            except Exception:
                pass
    fields["ts_ms"] = int(time.time() * 1000)
    return tk, fields


async def _subscribe_batch(ws, market_tickers, start_id=1):
    """Send one or more subscribe messages covering all given tickers."""
    sub_id = start_id
    for i in range(0, len(market_tickers), SUB_BATCH_SIZE):
        batch = market_tickers[i : i + SUB_BATCH_SIZE]
        await ws.send(json.dumps({
            "id": sub_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker"],
                "market_tickers": batch,
            },
        }))
        sub_id += 1
    return sub_id


async def _refresh_loop(ws, get_tickers, subscribed, next_id_box):
    """Every 60s, check the snapshot for newly-added market tickers and
    subscribe to them too. Runs until the outer WS loop cancels it."""
    while True:
        try:
            await asyncio.sleep(60)
            current = set(get_tickers())
            new = sorted(current - subscribed)
            if new:
                log.info("subscribing to %d new market tickers", len(new))
                next_id_box[0] = await _subscribe_batch(ws, new, next_id_box[0])
                subscribed.update(new)
                STATUS["subscribed"] = len(subscribed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("refresh loop error: %s", e)
            return


async def _price_flush_loop():
    """Flush buffered price updates to the database every N seconds.
    Runs as an asyncio task alongside the WS message loop.

    Deduplicates the buffer before writing — if a market got 10
    updates in a single flush cycle, only the latest snapshot is
    written. This dramatically reduces row count (and DB storage)
    without losing any resolution the chart actually needs (charts
    bucket into 30s windows so per-second ticks are redundant)."""
    while True:
        try:
            await asyncio.sleep(PRICE_FLUSH_INTERVAL)
            if not _price_buffer:
                continue
            # Swap the buffer so the WS loop can keep appending
            raw = _price_buffer[:]
            _price_buffer.clear()
            # Deduplicate: keep only the LAST update per market
            # ticker. Dict insertion order (Python 3.7+) preserves
            # chronological ordering, and later entries overwrite
            # earlier ones for the same key.
            deduped = {}
            for row in raw:
                tk = row.get("market_ticker")
                if tk:
                    deduped[tk] = row
            batch = list(deduped.values())
            # The Kalshi WS ticker channel doesn't include volume
            # or open-interest fields. Enrich from the REST cache
            # (rebuilt every 30 min by get_data) so the price-
            # history chart can render volume bars.
            try:
                from main import _cache
                vol_map = {}
                for rec in (_cache.get("data_all") or []):
                    for o in rec.get("outcomes", []):
                        tk2 = o.get("ticker")
                        if tk2:
                            vol_map[tk2] = (
                                o.get("_vol24h") or o.get("_vol") or 0
                            )
                for row in batch:
                    if row.get("volume") is None:
                        row["volume"] = vol_map.get(
                            row.get("market_ticker"))
            except Exception:
                pass  # non-critical enrichment
            try:
                from db import batch_insert_prices
                await batch_insert_prices(batch)
            except Exception as e:
                log.error("price flush error: %s", e)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("price flush loop error: %s", e)


async def run_ws_client(get_tickers):
    """Main background task. `get_tickers` is a zero-arg callable that
    returns the current list of market tickers we want live prices for.
    Waits for a non-empty list, connects, subscribes, pumps messages,
    and reconnects with exponential backoff on any failure."""
    if websockets is None or serialization is None:
        log.warning("websockets or cryptography missing — live prices disabled")
        return

    private_key = _load_private_key()
    if private_key is None:
        log.warning("KALSHI_PRIVATE_KEY missing or invalid — live prices disabled")
        return

    backoff = 1
    while True:
        # Wait for the REST snapshot to produce at least some tickers.
        tickers = []
        for _ in range(120):
            tickers = list(get_tickers())
            if tickers:
                break
            await asyncio.sleep(1)
        if not tickers:
            log.info("no tickers available yet; sleeping before retry")
            await asyncio.sleep(5)
            continue

        try:
            headers = _sign_headers(private_key)
            log.info("connecting to %s (%d tickers to subscribe)", WS_URL, len(tickers))
            async with websockets.connect(
                WS_URL,
                additional_headers=headers,
                max_size=8 * 1024 * 1024,
                ping_interval=15,
                ping_timeout=10,
            ) as ws:
                STATUS["connected"] = True
                STATUS["connected_since"] = datetime.now(timezone.utc).isoformat()
                STATUS["last_error"] = None
                backoff = 1

                subscribed = set(tickers)
                next_id_box = [1]
                next_id_box[0] = await _subscribe_batch(ws, tickers, next_id_box[0])
                STATUS["subscribed"] = len(subscribed)
                log.info("subscribed to %d market tickers", len(subscribed))

                refresh_task = asyncio.create_task(
                    _refresh_loop(ws, get_tickers, subscribed, next_id_box)
                )
                price_flush_task = asyncio.create_task(_price_flush_loop())
                try:
                    async for raw in ws:
                        STATUS["messages"] += 1
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            RAW_SAMPLES.append({"_unparsed": str(raw)[:500]})
                            continue
                        # Keep a rolling sample of what Kalshi actually
                        # sends so /api/ws_raw can show us the shape.
                        RAW_SAMPLES.append(msg)
                        parsed = _extract_update(msg)
                        if parsed:
                            tk, upd = parsed
                            cur = LIVE_PRICES.get(tk)
                            if cur is None:
                                LIVE_PRICES[tk] = upd
                            else:
                                cur.update(upd)
                            STATUS["updates"] += 1
                            # Push the delta to every browser
                            # subscriber watching this ticker.
                            try:
                                _broadcast_to_browsers(tk, upd)
                            except Exception as e:
                                log.debug("browser broadcast failed: %s", e)
                            # Buffer for DB price history
                            _price_buffer.append({
                                "market_ticker": tk,
                                "yes_bid": upd.get("yes_bid"),
                                "yes_ask": upd.get("yes_ask"),
                                "no_bid": upd.get("no_bid"),
                                "no_ask": upd.get("no_ask"),
                                "last_price": upd.get("last_price"),
                                "volume": upd.get("volume"),
                                "open_interest": upd.get("open_interest"),
                            })
                finally:
                    refresh_task.cancel()
                    price_flush_task.cancel()
                    try:
                        await refresh_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    try:
                        await price_flush_task
                    except (asyncio.CancelledError, Exception):
                        pass
        except Exception as e:
            STATUS["connected"] = False
            STATUS["last_error"] = f"{type(e).__name__}: {e}"
            log.error("ws error: %s — reconnecting in %ds", e, backoff)
        else:
            STATUS["connected"] = False

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)
