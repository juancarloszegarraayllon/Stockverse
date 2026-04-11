"""Kalshi WebSocket client that keeps LIVE_PRICES up-to-date.

Runs as an asyncio background task inside the FastAPI process. It
connects to Kalshi's v2 WebSocket feed, subscribes to the ticker_v2
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
    None if it's not a price update we care about. Coded defensively so
    we tolerate slight variations in Kalshi's payload shape."""
    if not isinstance(msg, dict):
        return None
    body = msg.get("msg") or msg.get("data") or msg
    if not isinstance(body, dict):
        return None
    tk = body.get("market_ticker") or body.get("ticker")
    if not tk:
        return None
    fields = {}
    for k in ("yes_bid", "yes_ask", "no_bid", "no_ask"):
        v = body.get(k)
        if v is not None:
            try:
                fields[k] = float(v)
            except Exception:
                pass
    if not fields:
        return None
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
                "channels": ["ticker_v2"],
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
                finally:
                    refresh_task.cancel()
                    try:
                        await refresh_task
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
