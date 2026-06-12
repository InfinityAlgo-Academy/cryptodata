import asyncio
import json
import urllib.request
from typing import Dict, List, Optional, Tuple

import config
from utils import get_logger

logger = get_logger("technical_indicators")

K_OPEN = 1
K_HIGH = 2
K_LOW = 3
K_CLOSE = 4
K_VOLUME = 5
K_QUOTE_VOL = 7


class TechnicalIndicatorStore:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._data: Dict[str, dict] = {}

    async def update(self, symbol: str, indicators: dict):
        async with self._lock:
            self._data[symbol] = indicators

    async def get_all(self) -> Dict[str, dict]:
        async with self._lock:
            return dict(self._data)

    async def get(self, symbol: str) -> Optional[dict]:
        async with self._lock:
            return self._data.get(symbol)


def sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    import pandas as pd
    series = pd.Series(values)
    return float(series.ewm(span=period, adjust=False).mean().iloc[-1])


def bollinger_bands(
    values: List[float], period: int = 20, num_std: float = 2.0
) -> Optional[Tuple[float, float, float]]:
    if len(values) < period:
        return None
    recent = values[-period:]
    m = sum(recent) / period
    variance = sum((x - m) ** 2 for x in recent) / period
    s = variance ** 0.5
    return m + num_std * s, m, m - num_std * s


def pivot_points(high: float, low: float, close: float) -> dict:
    p = (high + low + close) / 3
    hl = high - low
    return {
        "pivot": p,
        "r1": 2 * p - low,
        "r2": p + hl,
        "r3": p + 2 * hl,
        "s1": 2 * p - high,
        "s2": p - hl,
        "s3": p - 2 * hl,
    }


def atr_14(highs: List[float], lows: List[float], closes: List[float]) -> Optional[float]:
    period = 14
    if len(highs) < period + 1:
        return None
    trs = []
    for i in range(1, len(highs)):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        trs.append(max(hl, hc, lc))
    return ema(trs, period)


def avg_quote_volume(klines: List[list], n_candles: int = 24) -> Optional[float]:
    if len(klines) < n_candles:
        return None
    vols = [float(k[K_QUOTE_VOL]) for k in klines[-n_candles:]]
    return sum(vols) / len(vols) * (24 / n_candles) if vols else None


def vwap_from_klines(klines: List[list]) -> Optional[float]:
    if not klines:
        return None
    pv_sum = 0.0
    v_sum = 0.0
    for k in klines:
        tp = (float(k[K_HIGH]) + float(k[K_LOW]) + float(k[K_CLOSE])) / 3
        vol = float(k[K_VOLUME])
        pv_sum += tp * vol
        v_sum += vol
    return pv_sum / v_sum if v_sum else None


async def fetch_depth(symbol: str, limit: int = 100) -> Optional[Tuple[List, List]]:
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}"
    loop = asyncio.get_running_loop()
    def _fetch():
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        return data.get("bids", []), data.get("asks", [])
    try:
        return await loop.run_in_executor(None, _fetch)
    except Exception:
        return None


def find_walls(levels: List, cluster_pct: float = 0.2) -> Tuple[Optional[float], Optional[float]]:
    if not levels:
        return None, None
    parsed = sorted(
        [(float(p), float(q)) for p, q in levels if float(q) > 0],
        key=lambda x: x[0]
    )
    if not parsed:
        return None, None
    clusters = [[parsed[0]]]
    for p, q in parsed[1:]:
        ref = clusters[-1][-1][0]
        gap_pct = abs(p - ref) / ref * 100 if ref != 0 else 0.0
        if gap_pct <= cluster_pct:
            clusters[-1].append((p, q))
        else:
            clusters.append([(p, q)])
    scored = []
    for cl in clusters:
        total_qty = sum(q for _, q in cl)
        if total_qty == 0:
            continue
        avg_price = sum(p * q for p, q in cl) / total_qty
        scored.append((avg_price, total_qty))
    if not scored:
        return None, None
    scored.sort(key=lambda x: x[1], reverse=True)
    best = scored[0]
    return best[0], best[1]


async def fetch_klines(symbol: str, interval: str = "1h", limit: int = 100) -> List[list]:
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    loop = asyncio.get_running_loop()
    def _fetch():
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read().decode())
    return await loop.run_in_executor(None, _fetch)


async def compute_indicators_for_symbol(symbol: str) -> Optional[dict]:
    try:
        klines_1h = await fetch_klines(symbol, interval="1h", limit=210)
        closes_1h = [float(k[K_CLOSE]) for k in klines_1h]
        highs_1h = [float(k[K_HIGH]) for k in klines_1h]
        lows_1h = [float(k[K_LOW]) for k in klines_1h]

        result = {
            "sma20": sma(closes_1h, 20),
            "sma50": sma(closes_1h, 50),
            "sma200": sma(closes_1h, 200),
        }

        bb = bollinger_bands(closes_1h, 20, 2.0)
        if bb:
            result["bb_upper"], result["bb_middle"], result["bb_lower"] = bb

        lookback = 100
        if len(highs_1h) >= lookback:
            fib_high = max(highs_1h[-lookback:])
            fib_low = min(lows_1h[-lookback:])
            diff = fib_high - fib_low
            if diff > 0:
                result["fib_high"] = fib_high
                result["fib_low"] = fib_low
                result["fib_50"] = fib_low + 0.50 * diff
                result["fib_618"] = fib_low + 0.618 * diff
                result["fib_127"] = fib_high + 0.272 * diff
                result["fib_161"] = fib_high + 0.618 * diff

        last_24 = klines_1h[-24:] if len(klines_1h) >= 24 else klines_1h
        v = vwap_from_klines(last_24)
        if v is not None:
            result["vwap"] = v

        a = atr_14(highs_1h, lows_1h, closes_1h)
        if a is not None:
            result["atr"] = a

        av = avg_quote_volume(klines_1h, 24)
        if av is not None:
            result["avg_vol"] = av

        try:
            klines_1d = await fetch_klines(symbol, interval="1d", limit=3)
            if len(klines_1d) >= 2:
                prev = klines_1d[-2]
                pivots = pivot_points(
                    float(prev[K_HIGH]), float(prev[K_LOW]), float(prev[K_CLOSE])
                )
                result.update(pivots)
        except Exception:
            pass

        try:
            depth = await fetch_depth(symbol, 100)
            if depth:
                bids, asks = depth
                bp, bq = find_walls(bids)
                ap, aq = find_walls(asks)
                if bp is not None:
                    result["bid_wall_price"] = bp
                    result["bid_wall_qty"] = bq
                if ap is not None:
                    result["ask_wall_price"] = ap
                    result["ask_wall_qty"] = aq
        except Exception:
            pass

        return result

    except Exception as e:
        logger.debug("Failed indicators for %s: %s", symbol, e)
        return None


async def indicators_update_task(
    symbols: List[str],
    store: TechnicalIndicatorStore,
    shutdown_event: asyncio.Event,
) -> None:
    while not shutdown_event.is_set():
        async def fetch_one(sym: str):
            ind = await compute_indicators_for_symbol(sym)
            if ind:
                await store.update(sym, ind)

        tasks = [asyncio.create_task(fetch_one(s)) for s in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=120.0)
        except asyncio.TimeoutError:
            pass
