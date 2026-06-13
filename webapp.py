"""
webapp.py - Web Application for the Real-Time Crypto Market Scanner
===================================================================
Serves a professional dark-theme dashboard with real-time WebSocket updates.
Reuses all existing data pipeline modules.
"""

import asyncio
import json
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import pandas as pd

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

import config
import utils
import yahoo_finance
from market_data import MarketDataStore, DataFrameManager, TickData
from technical_indicators import TechnicalIndicatorStore, indicators_update_task
from websocket_client import BinanceWebSocketClient, fetch_top_symbols
from futures_extras import FuturesExtrasStore, futures_extras_task

log = utils.get_logger("webapp")

# ═══════════════════════════════════════════════════════════════
# RSI helpers (adapted from main.py)
# ═══════════════════════════════════════════════════════════════


def calculate_rsi(series: pd.Series, period: int) -> Optional[float]:
    if len(series) < period + 1:
        return None
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return float(val) if not pd.isna(val) else None


class RSIKlinesStore:
    def __init__(self):
        self.closes: Dict[str, List[float]] = {}
        self.lock = asyncio.Lock()

    async def update(self, symbol: str, closes: List[float]) -> None:
        async with self.lock:
            self.closes[symbol] = closes

    async def get_series(self, symbol: str, current_price: float) -> pd.Series:
        async with self.lock:
            if symbol not in self.closes or not self.closes[symbol]:
                return pd.Series([current_price])
            return pd.Series(self.closes[symbol][:-1] + [current_price])

    async def get_chart_data(self, symbol: str, current_price: float) -> Optional[Dict]:
        async with self.lock:
            if symbol not in self.closes or not self.closes[symbol]:
                return None
            raw = self.closes[symbol]
            closes = raw + [current_price]
            step = max(1, len(closes) // 100)
            sampled = closes[::step]
            if sampled[-1] != closes[-1]:
                sampled[-1] = closes[-1]
            mn = min(closes)
            mx = max(closes)
            rng = mx - mn if mx != mn else 1
            normalized = [round((c - mn) / rng * 100, 1) for c in sampled]
            return {
                "prices": [round(c, 2) for c in sampled],
                "normalized": normalized,
                "min": round(mn, 2),
                "max": round(mx, 2),
                "current": round(closes[-1], 2),
            }


async def compute_all_rsi_realtime(snapshot, rsi_store):
    rsi_dict = {}
    for tick in snapshot:
        sym = tick.get("symbol")
        price = tick.get("last_price")
        if sym and price is not None:
            s = await rsi_store.get_series(sym, float(price))
            rsi_dict[sym] = {"rsi_14": calculate_rsi(s, 14), "rsi_5": calculate_rsi(s, 5)}
    return rsi_dict


async def fetch_historical_closes(symbol, interval="1h", limit=100):
    import urllib.request
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    loop = asyncio.get_running_loop()
    def _fetch():
        with urllib.request.urlopen(url, timeout=5) as resp:
            return [float(c[4]) for c in json.loads(resp.read().decode())]
    return await loop.run_in_executor(None, _fetch)


async def rsi_klines_task(symbols, rsi_store, shutdown_event):
    while not shutdown_event.is_set():
        for sym in symbols:
            if shutdown_event.is_set():
                break
            try:
                closes = await fetch_historical_closes(sym, interval="1h", limit=100)
                await rsi_store.update(sym, closes)
            except Exception as e:
                log.error("Failed fetching 1h klines for %s: %s", sym, e)
            await asyncio.sleep(0.5)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
        except asyncio.TimeoutError:
            pass


# ═══════════════════════════════════════════════════════════════
# Signal scoring & Fib target logic (adapted from main.py)
# ═══════════════════════════════════════════════════════════════


def score_signal(rsi, bb_pct, fib_pct, ma20_dist, ma50_dist, ma200_dist, vwap_dist, rvol):
    score = 0
    if rsi is not None:
        if rsi < 10: score += 2
        elif rsi < 25: score += 1
        elif rsi > 90: score -= 2
        elif rsi > 75: score -= 1
    if bb_pct is not None:
        if bb_pct < 10: score += 2
        elif bb_pct < 25: score += 1
        elif bb_pct > 90: score -= 2
        elif bb_pct > 75: score -= 1
    if fib_pct is not None:
        if fib_pct < 23.6: score += 2
        elif fib_pct < 50: score += 1
        elif fib_pct > 78.6: score -= 2
        elif fib_pct > 61.8: score -= 1
    if ma20_dist is not None:
        if ma20_dist < -3: score += 2
        elif ma20_dist < -1: score += 1
        elif ma20_dist > 3: score -= 2
        elif ma20_dist > 1: score -= 1
    if ma50_dist is not None:
        if ma50_dist < -5: score += 2
        elif ma50_dist < -2: score += 1
        elif ma50_dist > 5: score -= 2
        elif ma50_dist > 2: score -= 1
    if ma200_dist is not None:
        if ma200_dist < -10: score += 2
        elif ma200_dist < -5: score += 1
        elif ma200_dist > 10: score -= 2
        elif ma200_dist > 5: score -= 1
    if vwap_dist is not None:
        if vwap_dist < -1: score += 1
        elif vwap_dist > 1: score -= 1
    if rvol is not None:
        if rvol > 1.5:
            score += 1 if score > 0 else -1 if score < 0 else 0
        elif rvol < 0.5:
            score = int(score * 0.5)
    return max(-14, min(14, score))


ACTION_LABELS = [
    (6, "STRONG BUY"), (3, "BUY"), (1, "LEAN BUY"),
    (-1, "LEAN SELL"), (-3, "SELL"), (-6, "STRONG SELL"),
]


def get_action_label(score):
    for threshold, label in ACTION_LABELS:
        if score >= threshold:
            return label
    return "HOLD"


def pick_fib_targets(price, ti, score):
    if price is None:
        return None
    fib_127 = ti.get("fib_127")
    fib_161 = ti.get("fib_161")
    fib_50 = ti.get("fib_50")
    fib_618 = ti.get("fib_618")
    fib_srcs = [v for v in [fib_50, fib_618, fib_127, fib_161] if v]

    def confluences(tgt, sources):
        return sum(1 for v in sources if v and abs(tgt - v) / tgt < 0.003)

    def pick(cands, is_buy):
        if not cands:
            return None
        stop_pool = [v for v in ([fib_618, fib_50] if is_buy else [fib_127, fib_161])
                     if v and (v < price if is_buy else v > price)]
        stop = (max(stop_pool) if stop_pool else price * (0.97 if is_buy else 1.03))
        scored = []
        for lbl, val in cands:
            risk = (price - stop) if is_buy else (stop - price)
            reward = (val - price) if is_buy else (price - val)
            if risk <= 0 or reward <= 0:
                continue
            con = confluences(val, fib_srcs)
            scored.append(((reward / risk) * (1 + 0.4 * con), val, con, lbl, reward / risk))
        if not scored:
            return None
        scored.sort(key=lambda x: x[0], reverse=True)
        _, tgt, con, lbl, rr = scored[0]
        return {"direction": "buy" if is_buy else "sell", "label": lbl,
                "price": round(tgt, 2), "confluence": con, "rr": round(rr, 1)}

    if score >= 2:
        bc = [(l, v) for l, v in [("F50", fib_50), ("F618", fib_618),
              ("F127", fib_127), ("F161", fib_161)] if v and v > price]
        return pick(bc, True)
    if score <= -2:
        sc = [(l, v) for l, v in [("F127", fib_127), ("F161", fib_161),
              ("F50", fib_50), ("F618", fib_618)] if v and v < price]
        return pick(sc, False)
    return None


# ═══════════════════════════════════════════════════════════════
# Connection Manager
# ═══════════════════════════════════════════════════════════════


class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()
        self.latest_data: Optional[dict] = None

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)
        if self.latest_data:
            await ws.send_json(self.latest_data)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, data: dict):
        self.latest_data = data
        dead = set()
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self.active.discard(ws)


manager = ConnectionManager()


# ═══════════════════════════════════════════════════════════════
# Display data computation
# ═══════════════════════════════════════════════════════════════


def qfmt_quote(qty, price):
    if qty is None or price is None or price <= 0:
        return None
    val = qty * price
    if val < 100:
        return None
    if val >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:.0f}"


def pfmt(v):
    if v is None:
        return None
    if v >= 1000:
        return f"{v:.2f}"
    return f"{v:.4f}"


async def compute_display_data(store, rsi_store, tech_store):
    snapshot = await store.get_snapshot()
    rsi_data = await compute_all_rsi_realtime(snapshot, rsi_store)
    tech_data = await tech_store.get_all()

    rows = sorted(snapshot, key=lambda t: float(t.get("volume") or 0), reverse=True)
    output = []

    for idx, tick in enumerate(rows):
        sym = tick.get("symbol", "???")
        pr = tick.get("last_price")
        pr_f = float(pr) if pr is not None else None
        pct = tick.get("price_change_pct") or 0.0
        vol = tick.get("volume")
        high = tick.get("high_24h")
        low = tick.get("low_24h")
        ts = tick.get("timestamp") or "--:--:--"
        bid = tick.get("bid_price")
        ask = tick.get("ask_price")
        bid_qty = tick.get("bid_qty")
        ask_qty = tick.get("ask_qty")

        ri = rsi_data.get(sym, {})
        rsi_v = ri.get("rsi_5")

        ti = tech_data.get(sym, {})
        ma20 = ti.get("sma20")
        ma50 = ti.get("sma50")
        ma200 = ti.get("sma200")
        vwap = ti.get("vwap")
        atr = ti.get("atr")
        avg_v = ti.get("avg_vol")
        bb_u = ti.get("bb_upper")
        bb_l = ti.get("bb_lower")
        fh = ti.get("fib_high")
        fl = ti.get("fib_low")

        d20 = (pr_f - ma20) / ma20 * 100 if pr_f is not None and ma20 and ma20 != 0 else None
        d50 = (pr_f - ma50) / ma50 * 100 if pr_f is not None and ma50 and ma50 != 0 else None
        d200 = (pr_f - ma200) / ma200 * 100 if pr_f is not None and ma200 and ma200 != 0 else None
        bbp = (pr_f - bb_l) / (bb_u - bb_l) * 100 if pr_f is not None and bb_u is not None and bb_l is not None and bb_u != bb_l else None
        fibp = (pr_f - fl) / (fh - fl) * 100 if pr_f is not None and fh is not None and fl is not None and fh != fl else None
        vd = (pr_f - vwap) / vwap * 100 if pr_f is not None and vwap and vwap != 0 else None
        rv = vol / avg_v if vol is not None and avg_v and avg_v != 0 else None

        score = score_signal(rsi_v, bbp, fibp, d20, d50, d200, vd, rv)
        action = get_action_label(score)
        targets = pick_fib_targets(pr_f, ti, score)
        trend = ("up" if rsi_v is not None and rsi_v > 50 else
                 "down" if rsi_v is not None and rsi_v < 50 else None)

        spread = ((ask - bid) / (pr_f or 1) * 10000) if bid is not None and ask is not None and pr_f and pr_f != 0 else None

        # OB
        bw_p, bw_q = ti.get("bid_wall_price"), ti.get("bid_wall_qty")
        aw_p, aw_q = ti.get("ask_wall_price"), ti.get("ask_wall_qty")

        ob_bid = None
        if bw_p is not None and bw_q is not None:
            rv = bw_q * (pr_f or 0)
            v = qfmt_quote(bw_q, pr_f)
            if v: ob_bid = {"price": pfmt(bw_p), "val": v, "raw": round(rv)}
        if not ob_bid and bid is not None and bid_qty is not None:
            rv = bid_qty * (pr_f or 0)
            v = qfmt_quote(bid_qty, pr_f)
            if v: ob_bid = {"price": pfmt(bid), "val": v, "raw": round(rv)}

        ob_ask = None
        if aw_p is not None and aw_q is not None:
            rv = aw_q * (pr_f or 0)
            v = qfmt_quote(aw_q, pr_f)
            if v: ob_ask = {"price": pfmt(aw_p), "val": v, "raw": round(rv)}
        if not ob_ask and ask is not None and ask_qty is not None:
            rv = ask_qty * (pr_f or 0)
            v = qfmt_quote(ask_qty, pr_f)
            if v: ob_ask = {"price": pfmt(ask), "val": v, "raw": round(rv)}

        # Whales
        whales_side, whales_val = None, None
        bv = (bw_q or 0) * (pr_f or 0)
        av = (aw_q or 0) * (pr_f or 0)
        if bv >= 100 or av >= 100:
            whales_side = "B" if bv >= av else "S"
            whales_val = qfmt_quote(bw_q if whales_side == "B" else aw_q, pr_f)
        else:
            bv2 = (bid_qty or 0) * (pr_f or 0)
            av2 = (ask_qty or 0) * (pr_f or 0)
            if bv2 >= 100 or av2 >= 100:
                whales_side = "B" if bv2 >= av2 else "S"
                whales_val = qfmt_quote(bid_qty if whales_side == "B" else ask_qty, pr_f)

        row = {
            "num": idx + 1, "symbol": sym, "score": score,
            "action": action.lower().replace(" ", "_"), "action_label": action,
            "targets": targets, "trend": trend,
            "price": pr_f, "chg_pct": round(pct, 2) if pct is not None else None,
            "spread_bps": round(spread, 1) if spread is not None else None,
            "ob_bid": ob_bid, "ob_ask": ob_ask, "vol": vol,
            "rvol_ratio": round(rv, 1) if rv is not None else None,
            "whales_side": whales_side, "whales_val": whales_val,
            "vwap_dist": round(vd, 2) if vd is not None else None,
            "ma20_dist": round(d20, 2) if d20 is not None else None,
            "ma50_dist": round(d50, 2) if d50 is not None else None,
            "ma200_dist": round(d200, 2) if d200 is not None else None,
            "bb_pct": round(bbp) if bbp is not None else None,
            "atr_pct": round(atr / pr_f * 100, 2) if atr is not None and pr_f and pr_f != 0 else None,
            "macd_hist": round(ti.get("macd_histogram"), 2) if ti.get("macd_histogram") is not None else None,
            "macd_div": ti.get("macd_divergence"),
            "fib_pct": round(fibp) if fibp is not None else None,
            "rsi5": round(rsi_v, 1) if rsi_v is not None else None,
            "rsi5_div": ti.get("rsi5_divergence"),
            "high": high, "low": low, "time": ts,
        }
        output.append(row)

    changes = [r["chg_pct"] for r in output if r["chg_pct"] is not None]
    gainers = sum(1 for c in changes if c > 0)
    losers = sum(1 for c in changes if c < 0)
    total_vol = sum(r["vol"] for r in output if r["vol"] is not None)
    avg_chg = sum(changes) / len(changes) if changes else 0.0
    best = max(output, key=lambda r: r["chg_pct"] or 0) if output else {}
    worst = min(output, key=lambda r: r["chg_pct"] or 0) if output else {}

    now = datetime.now(tz=timezone.utc).strftime("%H:%M:%S UTC")
    return {
        "type": "update", "time": now,
        "summary": {
            "gainers": gainers, "losers": losers, "total": len(output),
            "avg_chg": round(avg_chg, 2), "total_vol": total_vol,
            "best": {"symbol": best.get("symbol"), "chg_pct": best.get("chg_pct")},
            "worst": {"symbol": worst.get("symbol"), "chg_pct": worst.get("chg_pct")},
        },
        "rows": output,
    }


# ═══════════════════════════════════════════════════════════════
# Background task wrappers
# ═══════════════════════════════════════════════════════════════

store: MarketDataStore = None
df_manager: DataFrameManager = None
rsi_store: RSIKlinesStore = None
tech_store: TechnicalIndicatorStore = None
futures_extras_store: FuturesExtrasStore = None
ws_client: BinanceWebSocketClient = None
shutdown_event: asyncio.Event = None
bg_tasks: List[asyncio.Task] = []


async def broadcast_loop():
    while not shutdown_event.is_set():
        try:
            data = await compute_display_data(store, rsi_store, tech_store)
            await manager.broadcast(data)
        except Exception as e:
            log.error("Broadcast error: %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=config.DISPLAY_REFRESH_INTERVAL)
        except asyncio.TimeoutError:
            pass


async def dataframe_snapshot_task():
    while not shutdown_event.is_set():
        try:
            await df_manager.append_snapshot(await store.get_snapshot())
        except Exception as e:
            log.error("DataFrame snapshot error: %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass


async def yahoo_poll_task():
    symbols = ["GC=F"]
    while not shutdown_event.is_set():
        for sym in symbols:
            if shutdown_event.is_set():
                break
            try:
                data = await asyncio.get_running_loop().run_in_executor(None, yahoo_finance.fetch_current, sym)
                if data:
                    await store.update(sym, {
                        "symbol": sym, "last_price": data["price"],
                        "price_change_pct": data["change_pct"], "volume": data["volume"],
                        "high_24h": data["high"], "low_24h": data["low"],
                        "bid_price": None, "ask_price": None, "bid_qty": None, "ask_qty": None,
                        "timestamp": datetime.now(tz=timezone.utc).strftime("%H:%M:%S"),
                    })
            except Exception as e:
                log.debug("Yahoo poll error for %s: %s", sym, e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass


async def yahoo_indicators_task():
    symbols = ["GC=F"]
    while not shutdown_event.is_set():
        for sym in symbols:
            if shutdown_event.is_set():
                break
            try:
                klines = await asyncio.get_running_loop().run_in_executor(
                    None, yahoo_finance.fetch_ohlcv, sym, "1h", "3mo"
                )
                if klines:
                    closes = [float(k[4]) for k in klines]
                    await rsi_store.update(sym, closes)
                    from technical_indicators import compute_indicators_from_klines
                    await tech_store.update(sym, compute_indicators_from_klines(klines))
            except Exception as e:
                log.debug("Yahoo indicators error for %s: %s", sym, e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=120.0)
        except asyncio.TimeoutError:
            pass


# ═══════════════════════════════════════════════════════════════
# FastAPI App
# ═══════════════════════════════════════════════════════════════


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store, df_manager, rsi_store, tech_store, futures_extras_store, ws_client, shutdown_event, bg_tasks

    utils.setup_logging()
    log.info("Starting web application")

    # Discover symbols (same logic as main.py)
    symbols = await fetch_top_symbols(config.TOP_N_SYMBOLS + 30)
    symbols = [s for s in symbols if not any(st in s.upper() for st in config.STABLECOINS)]
    symbols = symbols[:config.TOP_N_SYMBOLS]
    symbols = [s for s in symbols if s != "XMRUSDT"]
    symbols = [s for s in symbols if re.match(r'^[A-Z0-9]{2,20}USDT$', s)]
    if "XAUTUSDT" not in symbols:
        symbols.append("XAUTUSDT")
    log.info("Top %d symbols: %s", len(symbols), symbols)

    extra = ["GC=F"]
    all_sym = symbols + extra

    store = MarketDataStore(all_sym)
    df_manager = DataFrameManager()
    rsi_store = RSIKlinesStore()
    tech_store = TechnicalIndicatorStore()
    futures_extras_store = FuturesExtrasStore()
    shutdown_event = asyncio.Event()

    ws_client = BinanceWebSocketClient(
        symbols=symbols, store=store, df_manager=df_manager, on_tick=None, shutdown_event=shutdown_event,
    )

    bg_tasks = [
        asyncio.create_task(ws_client.run(), name="ws"),
        asyncio.create_task(rsi_klines_task(symbols, rsi_store, shutdown_event), name="rsi"),
        asyncio.create_task(broadcast_loop(), name="broadcast"),
        asyncio.create_task(dataframe_snapshot_task(), name="df"),
        asyncio.create_task(indicators_update_task(symbols, tech_store, shutdown_event), name="indicators"),
        asyncio.create_task(yahoo_poll_task(), name="yahoo_poll"),
        asyncio.create_task(yahoo_indicators_task(), name="yahoo_ind"),
        asyncio.create_task(futures_extras_task(all_sym, futures_extras_store, shutdown_event), name="futures_extras"),
    ]

    log.info("All background tasks started")
    yield

    log.info("Shutting down…")
    shutdown_event.set()
    for t in bg_tasks:
        t.cancel()
    await asyncio.gather(*bg_tasks, return_exceptions=True)
    try:
        p = os.path.join(config.LOG_DIR, "final_snapshot.csv")
        await df_manager.export_csv(p)
        log.info("Snapshot exported to %s", p)
    except Exception as e:
        log.error("Export failed: %s", e)


app = FastAPI(title="Crypto Market Scanner", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def root():
    with open(os.path.join(static_dir, "index.html")) as f:
        return HTMLResponse(f.read())


@app.get("/chart/{symbol}")
async def chart_page(symbol: str):
    """Real-time candlestick chart with pivots & order book."""
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>""" + symbol + r""" — Chart</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;overflow:hidden;background:#131722;color:#d1d4dc;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}
body{display:flex;flex-direction:column}
.top{display:flex;align-items:center;gap:10px;padding:6px 14px;background:#1e222d;border-bottom:1px solid #2a2e39;flex-shrink:0;font-size:12px}
.top h2{font-size:13px;font-weight:600}
.top h2 span{color:#787b86;font-weight:400;font-size:11px}
#countdown{padding:2px 8px;border-radius:3px;background:rgba(41,98,255,0.12);color:#5b9cf5;font-family:monospace;font-size:11px;font-weight:600}
.top a{margin-left:auto;color:#2962ff;text-decoration:none;font-size:10px}
.loading{flex:1;display:flex;align-items:center;justify-content:center;color:#787b86;font-size:13px;gap:8px}
.spinner{width:16px;height:16px;border:2px solid #2a2e39;border-top-color:#2962ff;border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
#chart{flex:1;min-height:0;display:none}
#error{flex:1;display:none;align-items:center;justify-content:center;color:#f23645;font-size:13px;flex-direction:column;gap:8px}
#error a{color:#2962ff;text-decoration:none}
</style>
</head>
<body>
<div class="top">
  <h2>""" + symbol + r""" <span>— Real-Time</span></h2>
  <div id="countdown">--:--</div>
  <a href="https://www.tradingview.com/chart/?symbol=BINANCE:""" + symbol + r"""" target="_blank" rel="noopener">TradingView ↗</a>
</div>
<div class="loading" id="loading"><div class="spinner"></div>Loading 5000 candles&hellip;</div>
<div id="error"></div>
<div class="loading" id="realtime" style="display:none;flex:0;padding:6px;font-size:11px;color:#5b9cf5">Connecting real-time&hellip;</div>
<script src="https://unpkg.com/lightweight-charts@4.0.1/dist/lightweight-charts.standalone.production.js"></script>
<script>
const sym = '""" + symbol + r"""';
const isBinance = !sym.includes('=');

// ── Fetch 5000 1h candles from Binance ──
async function fetchKlines(endTime) {
  const url = 'https://api.binance.com/api/v3/klines?symbol=' + sym + '&interval=1h&limit=1000' + (endTime ? '&endTime=' + endTime : '');
  const res = await fetch(url);
  if (!res.ok) throw new Error('Binance API error ' + res.status);
  return await res.json();
}

async function loadChart() {
  try {
    let all = [];
    let endTime = Date.now();
    for (let i = 0; i < 5; i++) {
      const k = await fetchKlines(endTime);
      if (!k || !k.length) break;
      all = k.concat(all);
      endTime = k[0][0] - 1;
      if (k.length < 1000) break;
    }
    all = all.slice(-5000);
    if (all.length < 20) throw new Error('Not enough data (got ' + all.length + ' candles)');

    document.getElementById('loading').style.display = 'none';

    // ── Show chart container, then create chart ──
    const chartEl = document.getElementById('chart');
    chartEl.style.display = 'block';
    await new Promise(r => requestAnimationFrame(r));
    // ensure chartEl has rendered dimensions
    if (!chartEl || !chartEl.getBoundingClientRect) throw new Error('Chart container not found');

    const chart = LightweightCharts.createChart(chartEl, {
      layout: { background: { color: '#131722' }, textColor: '#d1d4dc' },
      grid: { vertLines: { color: '#1e222d' }, horzLines: { color: '#1e222d' } },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: '#2a2e39' },
      timeScale: { borderColor: '#2a2e39', timeVisible: true, secondsVisible: false },
      handleScroll: { vertTouchDrag: true },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#089981', downColor: '#f23645',
      borderUpColor: '#089981', borderDownColor: '#f23645',
      wickUpColor: '#089981', wickDownColor: '#f23645',
    });

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });

    // ── Set initial data ──
    const cdata = [], vdata = [];
    for (const k of all) {
      const t = Math.floor(k[0] / 1000);
      cdata.push({ time: t, open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]) });
      vdata.push({ time: t, value: parseFloat(k[5]), color: parseFloat(k[4]) >= parseFloat(k[1]) ? 'rgba(8,153,129,0.3)' : 'rgba(242,54,69,0.3)' });
    }
    candleSeries.setData(cdata);
    volumeSeries.setData(vdata);
    chart.timeScale().fitContent();
    chart.timeScale().scrollToPosition(-5);

    // ── Pivot lines ──
    let pivotLines = [];
    async function loadPivots() {
      try {
        const res = await fetch('/api/symbol/' + encodeURIComponent(sym));
        const d = await res.json();
        if (d.pivot_points && Object.keys(d.pivot_points).length) {
          pivotLines.forEach(l => chart.removePriceLine(l));
          pivotLines = [];
          const pivots = d.pivot_points;
          const colors = ['#f23645','#f77c5a','#f0b90b','#2962ff','#f0b90b','#f77c5a','#f23645'];
          const keys = ['r3','r2','r1','pivot','s1','s2','s3'];
          keys.forEach((k, i) => {
            if (pivots[k] != null) {
              pivotLines.push(candleSeries.createPriceLine({
                price: pivots[k], color: colors[i], lineWidth: 1, lineStyle: 2,
                axisLabelVisible: true, title: k.toUpperCase(),
              }));
            }
          });
        }
      } catch(e) { /* pivots not critical */ }
    }
    await loadPivots();

    // ── Order book bubbles (bid/ask lines) ──
    let obLines = [];
    async function loadOrderBook() {
      try {
        const res = await fetch('/api/symbol/' + encodeURIComponent(sym));
        const d = await res.json();
        obLines.forEach(l => chart.removePriceLine(l));
        obLines = [];
        if (d.orderbook) {
          const ob = d.orderbook;
          if (ob.bids && ob.bids.length) {
            const b = ob.bids[0];
            obLines.push(candleSeries.createPriceLine({
              price: b.p, color: 'rgba(8,153,129,0.6)', lineWidth: 2, lineStyle: 0,
              axisLabelVisible: true, title: 'Bid ' + b.p.toFixed(2),
            }));
          }
          if (ob.asks && ob.asks.length) {
            const a = ob.asks[0];
            obLines.push(candleSeries.createPriceLine({
              price: a.p, color: 'rgba(242,54,69,0.6)', lineWidth: 2, lineStyle: 0,
              axisLabelVisible: true, title: 'Ask ' + a.p.toFixed(2),
            }));
          }
        }
        if (d.order_book_walls) {
          if (d.order_book_walls.bid) {
            const w = d.order_book_walls.bid;
            obLines.push(candleSeries.createPriceLine({
              price: w.price, color: 'rgba(8,153,129,0.9)', lineWidth: 3, lineStyle: 0,
              axisLabelVisible: true, title: 'Wall $' + (w.value/1000).toFixed(0) + 'K',
            }));
          }
          if (d.order_book_walls.ask) {
            const w = d.order_book_walls.ask;
            obLines.push(candleSeries.createPriceLine({
              price: w.price, color: 'rgba(242,54,69,0.9)', lineWidth: 3, lineStyle: 0,
              axisLabelVisible: true, title: 'Wall $' + (w.value/1000).toFixed(0) + 'K',
            }));
          }
        }
      } catch(e) { /* ob not critical */ }
    }
    await loadOrderBook();
    setInterval(loadOrderBook, 5000);

    // ── Real-time WebSocket (Binance kline stream) ──
    if (isBinance) {
      document.getElementById('realtime').style.display = 'flex';
      let ws = null;
      let currentCandle = all[all.length - 1];
      let lastCandleTime = currentCandle[0];
      let closeTime = currentCandle[6];

      function connectWS() {
        try {
          if (ws) ws.close();
          ws = new WebSocket('wss://stream.binance.com:9443/ws/' + sym.toLowerCase() + '@kline_1h');
          document.getElementById('realtime').textContent = 'Real-time connected';

          ws.onmessage = (ev) => {
            const msg = JSON.parse(ev.data);
            if (msg.e !== 'kline') return;
            const k = msg.k;
            const t = Math.floor(k.t / 1000);
            const o = parseFloat(k.o), h = parseFloat(k.h), l = parseFloat(k.l), c = parseFloat(k.c);
            const v = parseFloat(k.v);
            const volColor = c >= o ? 'rgba(8,153,129,0.3)' : 'rgba(242,54,69,0.3)';
            closeTime = k.T;

            if (t === lastCandleTime) {
              // Update existing candle
              candleSeries.update({ time: t, open: o, high: h, low: l, close: c });
              volumeSeries.update({ time: t, value: v, color: volColor });
            } else {
              // New candle
              lastCandleTime = t;
              candleSeries.update({ time: t, open: o, high: h, low: l, close: c });
              volumeSeries.update({ time: t, value: v, color: volColor });
            }
          };

          ws.onclose = () => {
            document.getElementById('realtime').textContent = 'Reconnecting…';
            setTimeout(connectWS, 3000);
          };
          ws.onerror = () => { ws.close(); };
        } catch(e) {
          document.getElementById('realtime').textContent = 'WS error, retrying…';
          setTimeout(connectWS, 5000);
        }
      }

      // ── Countdown timer ──
      function updateCountdown() {
        if (!closeTime) return;
        const remaining = Math.max(0, Math.floor((closeTime - Date.now()) / 1000));
        const m = String(Math.floor(remaining / 60)).padStart(2, '0');
        const s = String(remaining % 60).padStart(2, '0');
        document.getElementById('countdown').textContent = m + ':' + s;
      }
      setInterval(updateCountdown, 200);

      connectWS();
    } else {
      document.getElementById('countdown').textContent = 'N/A';
    }

    // ── Resize handler ──
    window.addEventListener('resize', () => chart.applyOptions({ width: document.getElementById('chart').clientWidth }));

  } catch (e) {
    document.getElementById('loading').style.display = 'none';
    const err = document.getElementById('error');
    err.style.display = 'flex';
    err.innerHTML = '<div>Failed to load chart: ' + e.message + '</div>' +
      '<a href="https://www.tradingview.com/chart/?symbol=BINANCE:' + sym + '" target="_blank" rel="noopener">Open in TradingView instead &rarr;</a>';
  }
}
loadChart();
</script>
</body>
</html>"""
    return HTMLResponse(html)

@app.get("/api/health")
async def health():
    return {"status": "ok", "symbols": len(store.symbols) if store else 0, "clients": len(manager.active)}


@app.get("/api/data")
async def api_data():
    try:
        if manager.latest_data:
            return manager.latest_data
        if store and rsi_store and tech_store:
            return await compute_display_data(store, rsi_store, tech_store)
    except Exception as e:
        log.error("/api/data error: %s", e)
    return {"type": "update", "rows": [], "summary": {"gainers": 0, "losers": 0, "total": 0,
            "avg_chg": 0.0, "total_vol": 0, "best": None, "worst": None}}


def _fib_label(level: float) -> str:
    if level is None: return None
    # Map a fib level value to its label
    return None


@app.get("/api/symbol/{symbol}")
async def api_symbol(symbol: str):
    try:
        tick = await store.get_tick(symbol) if store else None
        tech = await tech_store.get(symbol) if tech_store else {}
        if not tech:
            tech = {}
        price = float(tick["last_price"]) if tick and tick.get("last_price") is not None else None

        # RSI from rsi_store
        rsi_5 = None
        rsi_14 = None
        if price is not None and rsi_store:
            series = await rsi_store.get_series(symbol, price)
            rsi_5 = calculate_rsi(series, 5)
            rsi_14 = calculate_rsi(series, 14)

        # Fib levels with labels
        fib_levels = []
        for label, key in [("0%", "fib_low"), ("23.6%", None), ("38.2%", None),
                           ("50%", "fib_50"), ("61.8%", "fib_618"),
                           ("100%", "fib_high"), ("127.2%", "fib_127"), ("161.8%", "fib_161")]:
            val = tech.get(key) if key else None
            if val is not None:
                dist = ((val - price) / price * 100) if price and price != 0 else None
                fib_levels.append({"label": label, "value": round(val, 4), "dist_pct": round(dist, 2) if dist is not None else None})

        # Pivot points
        pivots = {}
        for k in ["pivot", "r1", "r2", "r3", "s1", "s2", "s3"]:
            v = tech.get(k)
            if v is not None:
                pivots[k] = round(v, 4)

        # Moving averages
        mas = {}
        for k in ["sma20", "sma50", "sma200"]:
            v = tech.get(k)
            if v is not None:
                dist = ((price - v) / v * 100) if price and v != 0 else None
                mas[k] = {"value": round(v, 4), "dist_pct": round(dist, 2) if dist is not None else None}

        # Bollinger
        bb = {}
        for k in ["bb_upper", "bb_middle", "bb_lower"]:
            v = tech.get(k)
            if v is not None:
                bb[k] = round(v, 4)
        bb_pct = None
        if bb.get("bb_upper") and bb.get("bb_lower") and bb["bb_upper"] != bb["bb_lower"] and price is not None:
            bb_pct = round((price - bb["bb_lower"]) / (bb["bb_upper"] - bb["bb_lower"]) * 100, 1)

        # MACD
        macd = {}
        for k in ["macd", "macd_signal", "macd_histogram"]:
            v = tech.get(k)
            if v is not None:
                macd[k] = round(v, 2)
        macd["divergence"] = tech.get("macd_divergence")

        # Walls
        walls = {}
        for side in ["bid", "ask"]:
            p = tech.get(f"{side}_wall_price")
            q = tech.get(f"{side}_wall_qty")
            if p is not None and q is not None and price:
                val = q * price
                walls[side] = {"price": round(p, 4), "qty": round(q, 2), "value": round(val)}

        # Fear & Greed
        fg = futures_extras_store.get_fear_greed() if futures_extras_store else None

        # Futures extras
        fx = await futures_extras_store.get(symbol) if futures_extras_store else {}
        funding = fx.get("funding")
        oi = fx.get("open_interest")
        lsr = fx.get("long_short_ratio")
        liq = fx.get("liquidations")
        ob = fx.get("orderbook")

        # Chart data (sparkline)
        chart = await rsi_store.get_chart_data(symbol, price) if price is not None and rsi_store else None

        return {
            "symbol": symbol,
            "ticker": {
                "price": price,
                "change_pct": round(float(tick["price_change_pct"]), 2) if tick and tick.get("price_change_pct") is not None else None,
                "volume": tick.get("volume") if tick else None,
                "high": tick.get("high_24h") if tick else None,
                "low": tick.get("low_24h") if tick else None,
                "bid": tick.get("bid_price") if tick else None,
                "ask": tick.get("ask_price") if tick else None,
                "spread_bps": round((float(tick["ask_price"]) - float(tick["bid_price"])) / price * 10000, 1) if tick and tick.get("bid_price") is not None and tick.get("ask_price") is not None and price and price != 0 else None,
            } if tick else None,
            "rsi": {"rsi_5": round(rsi_5, 1) if rsi_5 is not None else None, "rsi_14": round(rsi_14, 1) if rsi_14 is not None else None},
            "rsi_divergence": tech.get("rsi5_divergence"),
            "moving_averages": mas,
            "bollinger": bb,
            "bollinger_pct": bb_pct,
            "vwap": round(tech["vwap"], 4) if tech.get("vwap") else None,
            "atr": round(tech["atr"], 4) if tech.get("atr") else None,
            "atr_pct": round(tech["atr"] / price * 100, 2) if tech.get("atr") and price and price != 0 else None,
            "avg_volume": round(tech["avg_vol"], 2) if tech.get("avg_vol") else None,
            "macd": macd,
            "fib_levels": fib_levels,
            "pivot_points": pivots,
            "order_book_walls": walls,
            "fear_greed": fg,
            "funding_rate": funding,
            "open_interest": oi,
            "long_short_ratio": lsr,
            "liquidations": liq,
            "orderbook": ob,
            "chart": chart,
        }
    except Exception as e:
        log.error("api_symbol error for %s: %s", symbol, e)
        return {"error": str(e)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)


if __name__ == "__main__":
    uvicorn.run("webapp:app", host="0.0.0.0", port=8080, log_level="info")
