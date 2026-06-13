"""
futures_extras.py - Futures data, Fear & Greed, Order Book depth
================================================================
Aggregates data from Binance Futures REST API and alternative.me
for the symbol detail modal. All data is cached in-memory and
refreshed periodically by a background task.
"""

import asyncio
import json
import logging
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

from utils import get_logger

log = get_logger("futures_extras")

BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com"
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1"


# ── helpers ────────────────────────────────────────────────────────────

async def _fetch_json(url: str, timeout: int = 8) -> Optional[Any]:
    loop = asyncio.get_running_loop()
    def _do():
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    try:
        return await loop.run_in_executor(None, _do)
    except Exception as e:
        log.debug("fetch_json error %s: %s", url.split("?")[0], e)
        return None


def _annualize_funding(rate: Optional[float]) -> Optional[float]:
    if rate is None:
        return None
    return round(rate * 3 * 365 * 100, 4)  # 8h→annualized→percent


# ── Store ──────────────────────────────────────────────────────────────


class FuturesExtrasStore:
    def __init__(self):
        self._lock = asyncio.Lock()
        # Per-symbol data
        self._futures: Dict[str, Dict] = {}
        # Global
        self._fear_greed: Optional[Dict] = None
        self._fear_greed_updated: Optional[datetime] = None
        # Last refresh time
        self._last_refresh: Optional[datetime] = None

    # ── Fear & Greed ─────────────────────────────────────────────────

    async def update_fear_greed(self) -> None:
        data = await _fetch_json(FEAR_GREED_URL)
        if data and "data" in data and len(data["data"]) > 0:
            async with self._lock:
                self._fear_greed = data["data"][0]
                self._fear_greed_updated = datetime.now(tz=timezone.utc)

    def get_fear_greed(self) -> Optional[Dict]:
        return self._fear_greed

    # ── Per-symbol futures data ──────────────────────────────────────

    async def update_symbol(self, symbol: str) -> None:
        if not symbol.endswith("USDT"):
            return
        # Fetch all in parallel
        results = await asyncio.gather(
            self._fetch_funding(symbol),
            self._fetch_open_interest(symbol),
            self._fetch_long_short_ratio(symbol),
            self._fetch_liquidations(symbol),
            self._fetch_orderbook(symbol),
            return_exceptions=True,
        )
        keys = ["funding", "open_interest", "long_short_ratio", "liquidations", "orderbook"]
        entry = {}
        any_ok = False
        for key, val in zip(keys, results):
            if isinstance(val, Exception):
                continue
            if val is not None:
                entry[key] = val
                any_ok = True
        if any_ok:
            async with self._lock:
                self._futures[symbol] = entry

    async def update_all(self, symbols: List[str]) -> None:
        await self.update_fear_greed()
        for sym in symbols:
            await self.update_symbol(sym)
        async with self._lock:
            self._last_refresh = datetime.now(tz=timezone.utc)
        log.info("FuturesExtrasStore refreshed %d symbols", len(symbols))

    async def get(self, symbol: str) -> Dict:
        async with self._lock:
            return dict(self._futures.get(symbol, {}))

    # ── Individual fetchers ──────────────────────────────────────────

    async def _fetch_funding(self, symbol: str) -> Optional[Dict]:
        url = f"{BINANCE_FUTURES_BASE}/fapi/v1/premiumIndex?symbol={symbol}"
        data = await _fetch_json(url)
        if not data:
            return None
        estimated = data.get("estimatedFundingRate")
        last = data.get("lastFundingRate")
        est_rate = float(estimated) if estimated else None
        last_rate = float(last) if last else None
        return {
            "estimated": est_rate,
            "last": last_rate,
            "annualized_est": _annualize_funding(est_rate),
            "annualized_last": _annualize_funding(last_rate),
            "mark_price": float(data.get("markPrice", 0)),
            "index_price": float(data.get("indexPrice", 0)),
            "next_funding_time": data.get("nextFundingTime"),
        }

    async def _fetch_open_interest(self, symbol: str) -> Optional[Dict]:
        url = f"{BINANCE_FUTURES_BASE}/fapi/v1/openInterest?symbol={symbol}"
        data = await _fetch_json(url)
        if not data:
            return None
        oi = float(data.get("openInterest", 0))
        return {"open_interest": oi}

    async def _fetch_long_short_ratio(self, symbol: str) -> Optional[Dict]:
        url = f"{BINANCE_FUTURES_BASE}/futures/data/globalLongShortAccountRatio?symbol={symbol}&period=5m&limit=1"
        data = await _fetch_json(url)
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        row = data[0]
        lsr = float(row.get("longShortRatio", 0))
        return {
            "long_short_ratio": round(lsr, 4),
            "long_account": float(row.get("longAccount", 0)),
            "short_account": float(row.get("shortAccount", 0)),
        }

    async def _fetch_liquidations(self, symbol: str) -> Optional[Dict]:
        # NOTE: allForceOrders requires Binance API key (signature).
        # This will silently return None without a valid key.
        url = f"{BINANCE_FUTURES_BASE}/fapi/v1/allForceOrders?symbol={symbol}&limit=20"
        data = await _fetch_json(url)
        if not data or not isinstance(data, list):
            return None
        total_long = 0.0
        total_short = 0.0
        total_vol = 0.0
        count = 0
        for order in data:
            if order.get("symbol") != symbol:
                continue
            side = order.get("side", "")
            qty = float(order.get("executedQty", 0))
            price = float(order.get("price", 0))
            vol = qty * price
            total_vol += vol
            count += 1
            if side == "SELL":
                total_long += vol
            else:
                total_short += vol
        if count == 0:
            return None
        return {
            "total_vol": round(total_vol, 0),
            "long_vol": round(total_long, 0),
            "short_vol": round(total_short, 0),
            "ratio": round(total_long / total_short, 2) if total_short > 0 else None,
            "count": count,
        }

    async def _fetch_orderbook(self, symbol: str) -> Optional[Dict]:
        url = f"{BINANCE_SPOT_BASE}/api/v3/depth?symbol={symbol}&limit=10"
        data = await _fetch_json(url)
        if not data:
            return None
        bids = [(float(p), float(q)) for p, q in data.get("bids", []) if float(q) > 0]
        asks = [(float(p), float(q)) for p, q in data.get("asks", []) if float(q) > 0]
        bid_vol = sum(q for _, q in bids)
        ask_vol = sum(q for _, q in asks)
        bid_notional = sum(p * q for p, q in bids)
        ask_notional = sum(p * q for p, q in asks)
        return {
            "top_bid": {"price": bids[0][0], "qty": bids[0][1]} if bids else None,
            "top_ask": {"price": asks[0][0], "qty": asks[0][1]} if asks else None,
            "total_bid_qty": round(bid_vol, 4),
            "total_ask_qty": round(ask_vol, 4),
            "total_bid_notional": round(bid_notional, 0),
            "total_ask_notional": round(ask_notional, 0),
            "bid_ask_ratio": round(bid_notional / ask_notional, 4) if ask_notional > 0 else None,
            "bids": [{"p": round(p, 4), "q": round(q, 4)} for p, q in bids[:5]],
            "asks": [{"p": round(p, 4), "q": round(q, 4)} for p, q in asks[:5]],
        }


# ── Background task ────────────────────────────────────────────────────


async def futures_extras_task(
    symbols: List[str],
    store: FuturesExtrasStore,
    shutdown_event: asyncio.Event,
) -> None:
    log.info("Futures extras task started")
    while not shutdown_event.is_set():
        try:
            await store.update_all(symbols)
        except Exception as e:
            log.error("futures_extras_task error: %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass
    log.info("Futures extras task ended")
