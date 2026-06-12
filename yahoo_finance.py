"""
yahoo_finance.py - Fetch current price & historical OHLCV from Yahoo Finance
=============================================================================
Provides synchronous fetchers (run in executor) for non-crypto assets like
gold futures (GC=F) that are not available on Binance.
"""

import json
import urllib.request
from typing import List, Optional, Dict, Tuple

from utils import get_logger

logger = get_logger("yahoo_finance")

_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"


def _request(url: str, timeout: int = 15) -> Optional[dict]:
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.debug("Yahoo Finance request failed: %s", e)
        return None


def fetch_current(symbol: str = "GC=F") -> Optional[dict]:
    """
    Return current price snapshot for *symbol* from Yahoo Finance.

    Returns a dict with keys: price, change_pct, volume, high, low
    or None on failure.
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d"
    data = _request(url, timeout=10)
    if not data:
        return None

    result = data.get("chart", {}).get("result", [])
    if not result:
        return None

    meta = result[0].get("meta", {})
    quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
    closes = quotes.get("close", [])

    last_close = None
    for i in range(len(closes) - 1, -1, -1):
        if closes[i] is not None:
            last_close = closes[i]
            break

    if last_close is None:
        last_close = meta.get("regularMarketPrice")

    if last_close is None:
        return None

    prev_close = meta.get("previousClose")
    if prev_close is None or prev_close == 0:
        prev_close = last_close

    change_pct = (last_close - prev_close) / prev_close * 100 if prev_close != 0 else 0.0
    day_high = meta.get("regularMarketDayHigh", last_close)
    day_low = meta.get("regularMarketDayLow", last_close)
    volume = meta.get("regularMarketVolume", 0) or 0

    return {
        "price": last_close,
        "change_pct": change_pct,
        "volume": float(volume),
        "high": day_high,
        "low": day_low,
    }


def fetch_ohlcv(symbol: str = "GC=F", interval: str = "1h", range_str: str = "3mo") -> List[list]:
    """
    Fetch historical OHLCV candles from Yahoo Finance.

    Returns a list of candles, each matching the Binance kline layout:
        [timestamp, open, high, low, close, volume, quote_vol]
    where price fields are strings and quote_vol is 0.0 (not applicable).

    Returns an empty list on failure.
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={interval}&range={range_str}"
    data = _request(url, timeout=15)
    if not data:
        return []

    result = data.get("chart", {}).get("result", [])
    if not result:
        return []

    timestamps = result[0].get("timestamp", [])
    quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
    opens = quotes.get("open", [])
    highs = quotes.get("high", [])
    lows = quotes.get("low", [])
    closes = quotes.get("close", [])
    volumes = quotes.get("volume", [])

    klines = []
    for i in range(len(timestamps)):
        if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes) or i >= len(volumes):
            continue
        if any(v is None for v in [timestamps[i], opens[i], highs[i], lows[i], closes[i], volumes[i]]):
            continue
        klines.append([
            timestamps[i],
            str(opens[i]),
            str(highs[i]),
            str(lows[i]),
            str(closes[i]),
            str(volumes[i]),
            0,       # close time (unused, Binance compat)
            "0.0",   # quote volume (not applicable)
        ])

    logger.info("Fetched %d OHLCV candles for %s (%s/%s)", len(klines), symbol, interval, range_str)
    return klines
