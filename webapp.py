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
    global store, df_manager, rsi_store, tech_store, ws_client, shutdown_event, bg_tasks

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
