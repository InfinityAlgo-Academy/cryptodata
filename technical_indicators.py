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


def macd_from_closes(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Optional[dict]:
    if len(closes) < slow + signal:
        return None
    import pandas as pd
    series = pd.Series(closes)
    ema_f = series.ewm(span=fast, adjust=False).mean()
    ema_s = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_f - ema_s
    sig_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - sig_line
    return {
        "macd": float(macd_line.iloc[-1]),
        "signal": float(sig_line.iloc[-1]),
        "histogram": float(hist.iloc[-1]),
        "hist_series": hist,
    }


def detect_macd_divergence(closes: List[float], lookback: int = 50) -> Optional[str]:
    if len(closes) < lookback + 10:
        return None
    import pandas as pd
    series = pd.Series(closes[-lookback:])
    ema_f = series.ewm(span=12, adjust=False).mean()
    ema_s = series.ewm(span=26, adjust=False).mean()
    macd_line = ema_f - ema_s
    sig = macd_line.ewm(span=9, adjust=False).mean()
    hist = (macd_line - sig).values
    prices = series.values

    pivots_high_p = []
    pivots_low_p = []
    pivots_high_m = []
    pivots_low_m = []

    for i in range(1, len(prices) - 1):
        if prices[i] > prices[i-1] and prices[i] > prices[i+1]:
            pivots_high_p.append((i, prices[i]))
        if prices[i] < prices[i-1] and prices[i] < prices[i+1]:
            pivots_low_p.append((i, prices[i]))
        if hist[i] > hist[i-1] and hist[i] > hist[i+1]:
            pivots_high_m.append((i, hist[i]))
        if hist[i] < hist[i-1] and hist[i] < hist[i+1]:
            pivots_low_m.append((i, hist[i]))

    # Classic bearish divergence: price higher high, MACD lower high
    if len(pivots_high_p) >= 2 and len(pivots_high_m) >= 2:
        lp_h, pp_h = pivots_high_p[-1], pivots_high_p[-2]
        lm_h, pm_h = pivots_high_m[-1], pivots_high_m[-2]
        if lp_h[1] > pp_h[1] and lm_h[1] < pm_h[1]:
            return "bearish"

    # Classic bullish divergence: price lower low, MACD higher low
    if len(pivots_low_p) >= 2 and len(pivots_low_m) >= 2:
        lp_l, pp_l = pivots_low_p[-1], pivots_low_p[-2]
        lm_l, pm_l = pivots_low_m[-1], pivots_low_m[-2]
        if lp_l[1] < pp_l[1] and lm_l[1] > pm_l[1]:
            return "bullish"

    return None


def detect_rsi_divergence(closes: List[float], lookback: int = 50) -> Optional[str]:
    if len(closes) < lookback + 10:
        return None
    import pandas as pd
    series = pd.Series(closes[-lookback:])
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=4, min_periods=5).mean()
    avg_loss = loss.ewm(com=4, min_periods=5).mean()
    rs = avg_gain / avg_loss
    rsi_vals = (100 - (100 / (1 + rs))).values
    prices = series.values

    pivots_high_p = []
    pivots_low_p = []
    pivots_high_r = []
    pivots_low_r = []

    for i in range(1, len(prices) - 1):
        if prices[i] > prices[i-1] and prices[i] > prices[i+1]:
            pivots_high_p.append((i, prices[i]))
        if prices[i] < prices[i-1] and prices[i] < prices[i+1]:
            pivots_low_p.append((i, prices[i]))
        if not pd.isna(rsi_vals[i]) and rsi_vals[i] > rsi_vals[i-1] and rsi_vals[i] > rsi_vals[i+1]:
            pivots_high_r.append((i, rsi_vals[i]))
        if not pd.isna(rsi_vals[i]) and rsi_vals[i] < rsi_vals[i-1] and rsi_vals[i] < rsi_vals[i+1]:
            pivots_low_r.append((i, rsi_vals[i]))

    if len(pivots_high_p) >= 2 and len(pivots_high_r) >= 2:
        if pivots_high_p[-1][1] > pivots_high_p[-2][1] and pivots_high_r[-1][1] < pivots_high_r[-2][1]:
            return "bearish"

    if len(pivots_low_p) >= 2 and len(pivots_low_r) >= 2:
        if pivots_low_p[-1][1] < pivots_low_p[-2][1] and pivots_low_r[-1][1] > pivots_low_r[-2][1]:
            return "bullish"

    return None


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


async def fetch_depth(symbol: str, limit: int = 5000) -> Optional[Tuple[List, List]]:
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


_depth_sem = asyncio.Semaphore(5)

async def fetch_depth(symbol: str, limit: int = 5000) -> Optional[Tuple[List, List]]:
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}"
    loop = asyncio.get_running_loop()
    def _fetch():
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        return data.get("bids", []), data.get("asks", [])
    try:
        async with _depth_sem:
            return await loop.run_in_executor(None, _fetch)
    except Exception:
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
