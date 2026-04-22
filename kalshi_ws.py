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

# Global reference to the active Kalshi WS connection so the browser
# /ws/prices handler can trigger on-demand subscriptions to expensive
# channels (orderbook_delta, trade) for specific tickers.
_active_ws = None
_active_next_id = [1000]  # separate ID range from ticker subs

# On-demand channel subscriptions tracked per-ticker so we can
# unsubscribe when no browser clients need them anymore.
# Format: { (channel, ticker): set_of_browser_subscriber_ids }
_ondemand_subs: dict = {}


async def subscribe_ondemand(channel: str, ticker: str, sub_id: int):
    """Subscribe to an expensive channel (orderbook_delta, trade) for
    a single ticker. Called by the browser WS handler when a user
    opens the Order Book or Large Capital Flow section. Reference-
    counted: first subscriber triggers the Kalshi sub, subsequent ones
    just piggyback. Last unsubscribe sends the Kalshi unsub."""
    key = (channel, ticker.upper())
    if key not in _ondemand_subs:
        _ondemand_subs[key] = set()
    _ondemand_subs[key].add(sub_id)
    # First subscriber for this (channel, ticker) → subscribe on Kalshi
    if len(_ondemand_subs[key]) == 1 and _active_ws:
        try:
            _active_next_id[0] = await _subscribe_batch(
                _active_ws, [ticker.upper()], _active_next_id[0],
                channels=[channel],
            )
            log.info("on-demand subscribe: %s / %s", channel, ticker)
        except Exception as e:
            log.warning("on-demand subscribe failed: %s", e)


async def unsubscribe_ondemand(channel: str, ticker: str, sub_id: int):
    """Unsubscribe from an expensive channel when the last browser
    viewer closes the section."""
    key = (channel, ticker.upper())
    if key in _ondemand_subs:
        _ondemand_subs[key].discard(sub_id)
        if not _ondemand_subs[key]:
            del _ondemand_subs[key]
            if _active_ws:
                try:
                    _active_next_id[0] = await _unsubscribe_batch(
                        _active_ws, [ticker.upper()], _active_next_id[0],
                        channels=[channel],
                    )
                    log.info("on-demand unsubscribe: %s / %s", channel, ticker)
                except Exception as e:
                    log.warning("on-demand unsubscribe failed: %s", e)


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


def _broadcast_to_browsers(ticker: str, update: dict, msg_type: str = "price"):
    """Non-blocking fan-out of a single-ticker update to every browser
    subscriber that cares about this ticker. Drops the message on any
    subscriber whose queue is full (slow consumer protection)."""
    if not _browser_subscribers:
        return
    payload = {"type": msg_type, "ticker": ticker, "data": update}
    for sub in list(_browser_subscribers):
        if ticker not in sub.tickers:
            continue
        try:
            sub.queue.put_nowait(payload)
        except asyncio.QueueFull:
            pass


def _extract_orderbook_delta(msg):
    """Parse an orderbook_delta message. Returns (ticker, delta_dict)
    or None. Delta contains yes/no arrays of [price, qty] pairs
    representing the NEW state at each price level (qty=0 means
    remove that level)."""
    if not isinstance(msg, dict):
        return None
    body = msg.get("msg") or msg.get("data") or msg
    if not isinstance(body, dict):
        return None
    tk = body.get("market_ticker") or body.get("ticker")
    if not tk:
        return None
    delta = {}
    for side in ("yes", "no"):
        raw = body.get(side) or body.get(f"{side}_dollars") or []
        if raw:
            levels = []
            for lv in raw:
                if isinstance(lv, (list, tuple)) and len(lv) >= 2:
                    try:
                        p = lv[0]
                        if isinstance(p, str):
                            p = round(float(p) * 100)
                        else:
                            p = round(float(p))
                        q = float(lv[1])
                        levels.append([p, q])
                    except Exception:
                        pass
            if levels:
                delta[side] = levels
    if not delta:
        return None
    return (tk, delta)


def _extract_trade(msg):
    """Parse a trade channel message. Returns (ticker, trade_dict) or
    None. The trade dict includes side, price (cents), count, cost,
    and timestamp."""
    if not isinstance(msg, dict):
        return None
    body = msg.get("msg") or msg.get("data") or msg
    if not isinstance(body, dict):
        return None
    tk = body.get("market_ticker") or body.get("ticker")
    if not tk:
        return None
    taker_side = body.get("taker_side", "")
    count = body.get("count_fp") or body.get("count") or 0
    if isinstance(count, str):
        try:
            count = float(count)
        except Exception:
            count = 0
    yes_price = body.get("yes_price_dollars") or body.get("yes_price") or 0
    no_price = body.get("no_price_dollars") or body.get("no_price") or 0
    if isinstance(yes_price, str):
        yes_price = float(yes_price)
    if isinstance(no_price, str):
        no_price = float(no_price)
    if yes_price > 1:
        yes_price = yes_price / 100.0
    if no_price > 1:
        no_price = no_price / 100.0
    if taker_side == "yes":
        cost = round(yes_price * count, 2)
        price_cents = round(yes_price * 100)
        side = "YES"
    else:
        cost = round(no_price * count, 2)
        price_cents = round(no_price * 100)
        side = "NO"
    created = body.get("created_time") or body.get("trade_time") or ""
    return (tk, {
        "side": side,
        "price": price_cents,
        "contracts": count,
        "cost": cost,
        "time": created,
        "trade_id": body.get("trade_id", ""),
    })


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
                # Round to nearest cent to avoid IEEE-754 artifacts
                # like float("0.14") * 100 == 14.000000000000002.
                fields[k_out] = round(float(v) * 100)
            except Exception:
                pass
    # Raw cents fields as a fallback.
    for k in ("yes_bid", "yes_ask", "no_bid", "no_ask"):
        if k in fields:
            continue
        v = body.get(k)
        if v is not None:
            try:
                fields[k] = round(float(v))
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
    # Kalshi's price_dollars can represent either a YES or NO side
    # trade. Always normalize to the YES perspective by picking
    # whichever interpretation is closest to yes_bid. Use the current
    # tick's yes_bid if present, otherwise fall back to the stored
    # value in LIVE_PRICES (handles trade-only ticks that don't
    # include bid/ask data).
    if "last_price" in fields:
        yb_ref = fields.get("yes_bid")
        if yb_ref is None:
            stored = LIVE_PRICES.get(tk)
            if stored:
                yb_ref = stored.get("yes_bid")
        if yb_ref is not None:
            lp = fields["last_price"]
            flipped = 100 - lp
            if abs(flipped - yb_ref) < abs(lp - yb_ref):
                fields["last_price"] = flipped
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


async def _subscribe_batch(ws, market_tickers, start_id=1, channels=None):
    """Send one or more subscribe messages covering all given tickers."""
    if channels is None:
        channels = ["ticker"]
    sub_id = start_id
    for i in range(0, len(market_tickers), SUB_BATCH_SIZE):
        batch = market_tickers[i : i + SUB_BATCH_SIZE]
        await ws.send(json.dumps({
            "id": sub_id,
            "cmd": "subscribe",
            "params": {
                "channels": channels,
                "market_tickers": batch,
            },
        }))
        sub_id += 1
    return sub_id


async def _unsubscribe_batch(ws, market_tickers, start_id=1, channels=None):
    """Unsubscribe from specific channels for specific tickers."""
    if channels is None:
        channels = ["ticker"]
    sub_id = start_id
    for i in range(0, len(market_tickers), SUB_BATCH_SIZE):
        batch = market_tickers[i : i + SUB_BATCH_SIZE]
        await ws.send(json.dumps({
            "id": sub_id,
            "cmd": "unsubscribe",
            "params": {
                "channels": channels,
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
                global _active_ws
                _active_ws = ws
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
                        # Detect message type. Try orderbook_delta and
                        # trade FIRST — if the price parser runs first,
                        # it can misinterpret orderbook snapshot fields
                        # as stale price ticks, causing probability
                        # bouncing when the Order Book section is open.
                        handled = False
                        # 1) Orderbook delta — check first so OB
                        #    snapshots don't leak into price updates.
                        ob_parsed = _extract_orderbook_delta(msg)
                        if ob_parsed:
                            tk, delta = ob_parsed
                            try:
                                _broadcast_to_browsers(tk, delta, "orderbook_delta")
                            except Exception as e:
                                log.debug("ob delta broadcast failed: %s", e)
                            handled = True
                        # 2) Trade channel
                        if not handled:
                            tr_parsed = _extract_trade(msg)
                            if tr_parsed:
                                tk, trade = tr_parsed
                                try:
                                    _broadcast_to_browsers(tk, trade, "trade")
                                except Exception as e:
                                    log.debug("trade broadcast failed: %s", e)
                                handled = True
                        # 3) Ticker channel (price updates) — last so
                        #    OB/trade messages can't be misinterpreted.
                        if not handled:
                            parsed = _extract_update(msg)
                            if parsed:
                                tk, upd = parsed
                                cur = LIVE_PRICES.get(tk)
                                if cur is None:
                                    LIVE_PRICES[tk] = upd
                                else:
                                    cur.update(upd)
                                STATUS["updates"] += 1
                                try:
                                    _broadcast_to_browsers(tk, upd, "price")
                                except Exception as e:
                                    log.debug("browser broadcast failed: %s", e)
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
                                handled = True
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
            _active_ws = None
            _ondemand_subs.clear()
            STATUS["connected"] = False
            STATUS["last_error"] = f"{type(e).__name__}: {e}"
            log.error("ws error: %s — reconnecting in %ds", e, backoff)
        else:
            _active_ws = None
            _ondemand_subs.clear()
            STATUS["connected"] = False

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)
