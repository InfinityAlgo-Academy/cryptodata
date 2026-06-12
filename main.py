"""
main.py - Ultra-Professional Crypto Market Scanner with Rich TUI
================================================================
A highly polished, flicker-free terminal interface built with 'rich'.
Features:
  · True layout engine (Header, Summary, Main Table, Footer)
  · Seamless Live updates
  · Color-coded trends and dynamic styling
  · Clean code architecture
"""

import asyncio
import os
import re
import signal
import sys
import time
import json
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

import config
import utils
import yahoo_finance
from market_data import MarketDataStore, DataFrameManager, TickData
from technical_indicators import TechnicalIndicatorStore, indicators_update_task, compute_indicators_from_klines
from websocket_client import BinanceWebSocketClient, fetch_top_symbols

# Logger setup: file only, no console output to interfere with Rich UI
logger = utils.setup_logging()
log = utils.get_logger("main")


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
    """Stores historical 1-hour close prices for accurate RSI calculation."""
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
            # Exclude the last candle (unclosed) and append the live current price
            return pd.Series(self.closes[symbol][:-1] + [current_price])


async def compute_all_rsi_realtime(snapshot: List[TickData], rsi_store: RSIKlinesStore) -> Dict[str, Dict[str, Optional[float]]]:
    rsi_dict = {}
    for tick in snapshot:
        symbol = tick.get("symbol")
        current_price = tick.get("last_price")
        if symbol and current_price is not None:
            prices_series = await rsi_store.get_series(symbol, float(current_price))
            rsi_14 = calculate_rsi(prices_series, 14)
            rsi_5 = calculate_rsi(prices_series, 5)
            rsi_dict[symbol] = {"rsi_14": rsi_14, "rsi_5": rsi_5}
    return rsi_dict


# ═══════════════════════════════════════════════════════════════════════════════
#  Price Direction Tracker (▲ / ▼ / ─)
# ═══════════════════════════════════════════════════════════════════════════════



#  UI Components (Rich Renderables)
# ═══════════════════════════════════════════════════════════════════════════════


class ScannerUI:
    """Manages the Rich Layout and constructs all UI components per frame."""

    def __init__(self) -> None:
        self.console = Console()
        self.layout = self._make_layout()
        self.start_t = time.monotonic()
        self.spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self.frame = 0

    def _make_layout(self) -> Layout:
        """Create the main grid structure."""
        layout = Layout(name="root")
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="summary", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3),
        )
        return layout

    def update(self, snapshot: List[TickData], connected: bool, rsi_data: Dict[str, Dict[str, Optional[float]]] = None, tech_data: Dict[str, dict] = None) -> Layout:
        """Rebuild all components with the latest data and return the layout."""
        self.frame += 1

        # Sort data by volume
        rows = sorted(snapshot, key=lambda t: float(t.get("volume") or 0), reverse=True)

        self.layout["header"].update(self._build_header(connected))
        self.layout["summary"].update(self._build_summary(rows))
        self.layout["main"].update(self._build_table(rows, rsi_data or {}, tech_data or {}))
        self.layout["footer"].update(self._build_footer(len(rows)))

        return self.layout

    def _build_header(self, connected: bool) -> Panel:
        """Builds the top header panel."""
        now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        status = (
            Text("● LIVE", style="bold green")
            if connected
            else Text("◌ RECONNECTING", style="bold red")
        )
        spin_ch = self.spinner[self.frame % len(self.spinner)]

        # We use a Table to align left, center, and right within the Panel
        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right", ratio=1)

        title = Text("🚀 REAL-TIME CRYPTO MARKET SCANNER", style="bold cyan")
        grid.add_row(
            title,
            Group(status, Text(f" Binance WS {spin_ch}", style="dim")),
            Text(now_str, style="bold yellow"),
        )

        return Panel(grid, style="cyan")

    def _build_summary(self, rows: List[TickData]) -> Panel:
        """Builds the market summary breadth and statistics panel."""
        changes = [float(t.get("price_change_pct") or 0) for t in rows]
        gainers = sum(1 for c in changes if c > 0)
        losers = sum(1 for c in changes if c < 0)
        total_vol = sum(float(t.get("volume") or 0) for t in rows)

        if changes:
            avg_chg = sum(changes) / len(changes)
            best = max(rows, key=lambda t: float(t.get("price_change_pct") or 0))
            worst = min(rows, key=lambda t: float(t.get("price_change_pct") or 0))
        else:
            avg_chg = 0.0
            best = worst = {}

        # Breadth bar
        width = 20
        total = len(rows) or 1
        g_w = int(gainers / total * width)
        l_w = int(losers / total * width)
        n_w = width - g_w - l_w

        bar = Text()
        bar.append("█" * g_w, style="green")
        bar.append("▒" * n_w, style="dim white")
        bar.append("█" * l_w, style="red")

        avg_style = "green" if avg_chg >= 0 else "red"

        grid = Table.grid(expand=True)
        for _ in range(5):
            grid.add_column(justify="center")

        grid.add_row(
            Text.assemble(("BREADTH ", "dim"), bar, (f" ▲{gainers} ▼{losers}", "bold")),
            Text.assemble(
                ("AVG CHG ", "dim"), (f"{avg_chg:+.2f}%", f"bold {avg_style}")
            ),
            Text.assemble(
                ("VOL ", "dim"), (f"{utils.format_volume(total_vol)}", "bold magenta")
            ),
            Text.assemble(
                ("BEST ", "dim"),
                (f"{best.get('symbol', '─'):<8} ", "bold white"),
                (f"{best.get('price_change_pct', 0):+.2f}%", "bold green"),
            ),
            Text.assemble(
                ("WORST ", "dim"),
                (f"{worst.get('symbol', '─'):<8} ", "bold white"),
                (f"{worst.get('price_change_pct', 0):+.2f}%", "bold red"),
            ),
        )
        return Panel(grid, border_style="dim", padding=(0, 2))

    def _build_table(self, rows: List[TickData], rsi_data: Dict[str, Dict[str, Optional[float]]], tech_data: Dict[str, dict]) -> Table:
        """Builds the main Rich Table for displaying ticking symbols."""
        table = Table(
            expand=True,
            show_edge=False,
            show_lines=False,
            row_styles=["none", "dim"],
            border_style="dim",
        )

        table.add_column("№", justify="center", style="dim", width=3)
        table.add_column("SYMBOL", justify="center", style="bold white")
        table.add_column("ACT", justify="center")
        table.add_column("TGT", justify="center")
        table.add_column("TREND", justify="center", width=5)
        table.add_column("PRICE", justify="center", style="bold yellow")
        table.add_column("CHG%", justify="center")
        table.add_column("SPR", justify="center")
        table.add_column("OB", justify="center")
        table.add_column("VOL", justify="center", style="magenta")
        table.add_column("R VOL", justify="center")
        table.add_column("WHALES", justify="center")
        table.add_column("VWAP", justify="center")
        table.add_column("MA20", justify="center")
        table.add_column("MA50", justify="center")
        table.add_column("MA200", justify="center")
        table.add_column("BB%", justify="center")
        table.add_column("ATR", justify="center")
        table.add_column("MACD", justify="center")
        table.add_column("DIV", justify="center", width=5)
        table.add_column("FIB", justify="center")
        table.add_column("RSI", justify="center")
        table.add_column("HIGH", justify="center", style="white")
        table.add_column("LOW", justify="center", style="white")
        table.add_column("TIME", justify="center", style="dim")

        def format_rsi(val: Optional[float]) -> Text:
            if val is None:
                return Text("N/A", style="dim")
            color = "bold red" if val > 70 else "bold green" if val < 30 else "white"
            return Text(f"{val:.1f}", style=color)

        def format_ma_dist(price: Optional[float], ma_val: Optional[float]) -> Text:
            if price is None or ma_val is None or ma_val == 0:
                return Text("N/A", style="dim")
            dist = (price - ma_val) / ma_val * 100
            color = "bold green" if dist >= 0 else "bold red"
            return Text(f"{dist:+.2f}%", style=color)

        def format_bb(price: Optional[float], upper: Optional[float], lower: Optional[float]) -> Text:
            if price is None or upper is None or lower is None or upper == lower:
                return Text("N/A", style="dim")
            pct = (price - lower) / (upper - lower) * 100
            pct = max(0, min(100, pct))
            color = "bold red" if pct > 85 else "bold green" if pct < 15 else "white"
            return Text(f"{pct:.0f}%", style=color)

        def format_fib(price: Optional[float], fib_low: Optional[float], fib_high: Optional[float]) -> Text:
            if price is None or fib_low is None or fib_high is None or fib_high == fib_low:
                return Text("N/A", style="dim")
            pct = (price - fib_low) / (fib_high - fib_low) * 100
            pct = max(0, min(100, pct))
            if 50 <= pct <= 61.8:
                color = "bold green"
            elif pct < 50:
                color = "cyan"
            elif pct > 78.6:
                color = "bold red"
            else:
                color = "yellow"
            return Text(f"{pct:.0f}%", style=color)

        def format_spread(bid: Optional[float], ask: Optional[float], price: Optional[float]) -> Text:
            if bid is None or ask is None or price is None or price == 0:
                return Text("N/A", style="dim")
            spread_bps = (ask - bid) / price * 10000
            color = "bold green" if spread_bps < 5 else "yellow" if spread_bps < 20 else "bold red"
            return Text(f"{spread_bps:.1f}", style=color)

        def format_rvol(current_vol: Optional[float], avg_vol: Optional[float]) -> Text:
            if current_vol is None or avg_vol is None or avg_vol == 0:
                return Text("N/A", style="dim")
            ratio = current_vol / avg_vol
            color = "bold green" if ratio > 1.5 else "yellow" if ratio > 0.7 else "dim"
            return Text(f"{ratio:.1f}x", style=color)

        def format_ob(price: Optional[float], ti: dict, bid: Optional[float]=None, bid_qty: Optional[float]=None, ask: Optional[float]=None, ask_qty: Optional[float]=None) -> Text:
            def qfmt(v):
                if v is None: return "—"
                return f"{v/1_000_000:.2f}M"
            def pfmt(v):
                if v is None: return "—"
                if v >= 1000: return f"{v:.2f}"
                return f"{v:.4f}"
            bw_p = ti.get("bid_wall_price")
            bw_q = ti.get("bid_wall_qty")
            aw_p = ti.get("ask_wall_price")
            aw_q = ti.get("ask_wall_qty")
            has_walls = bw_p is not None or aw_p is not None
            bar = Text("│", style="dim")
            if has_walls:
                parts = []
                if bw_p is not None and bw_q is not None:
                    parts.append(Text(f"B {pfmt(bw_p)} {qfmt(bw_q)}", style="bold green"))
                else:
                    parts.append(Text("B —", style="dim"))
                parts.append(bar)
                if aw_p is not None and aw_q is not None:
                    parts.append(Text(f"S {pfmt(aw_p)} {qfmt(aw_q)}", style="bold red"))
                else:
                    parts.append(Text("S —", style="dim"))
                return Text.assemble(*parts)
            if bid is not None and bid_qty is not None and ask is not None and ask_qty is not None:
                return Text.assemble(
                    Text(f"{pfmt(bid)} {qfmt(bid_qty)}", style="green"),
                    bar,
                    Text(f"{pfmt(ask)} {qfmt(ask_qty)}", style="red"),
                )
            return Text("—", style="dim")

        def format_vwap(price: Optional[float], vwap: Optional[float]) -> Text:
            if price is None or vwap is None or vwap == 0:
                return Text("N/A", style="dim")
            dist = (price - vwap) / vwap * 100
            color = "bold green" if dist >= 0 else "bold red"
            return Text(f"{dist:+.2f}%", style=color)

        def format_atr(atr_val: Optional[float], price: Optional[float]) -> Text:
            if atr_val is None or price is None or price == 0:
                return Text("N/A", style="dim")
            pct = atr_val / price * 100
            color = "bold yellow" if pct > 5 else "white" if pct > 2 else "dim"
            return Text(f"{pct:.2f}%", style=color)

        def format_macd_hist(hist: Optional[float]) -> Text:
            if hist is None:
                return Text("N/A", style="dim")
            color = "bold green" if hist >= 0 else "bold red"
            return Text(f"{hist:+.2f}", style=color)

        def format_divergence(div: Optional[str]) -> Text:
            if div is None:
                return Text("—", style="dim")
            if div == "bullish":
                return Text("⬆", style="bold green")
            return Text("⬇", style="bold red")

        def format_whales(ti: dict, bid_qty: Optional[float]=None, ask_qty: Optional[float]=None) -> Text:
            bw_q = ti.get("bid_wall_qty")
            aw_q = ti.get("ask_wall_qty")
            if bw_q is not None or aw_q is not None:
                max_q = max(bw_q or 0, aw_q or 0)
                side = "B" if (bw_q or 0) >= (aw_q or 0) else "S"
                qfmt = f"{max_q/1_000_000:.2f}M" if max_q >= 1_000_000 else f"{max_q/1_000:.2f}K"
                color = "bold green" if side == "B" else "bold red"
                return Text(f"{side} {qfmt}", style=color)
            if bid_qty is not None or ask_qty is not None:
                max_q = max(bid_qty or 0, ask_qty or 0)
                side = "B" if (bid_qty or 0) >= (ask_qty or 0) else "S"
                qfmt = f"{max_q/1_000_000:.2f}M" if max_q >= 1_000_000 else f"{max_q/1_000:.2f}K"
                color = "bold green" if side == "B" else "bold red"
                return Text(f"{side} {qfmt}", style=color)
            return Text("—", style="dim")

        def score_signal(rsi: Optional[float], bb_pct: Optional[float], fib_pct: Optional[float],
                         ma20_dist: Optional[float], ma50_dist: Optional[float],
                         ma200_dist: Optional[float], vwap_dist: Optional[float],
                         rvol: Optional[float]) -> int:
            score = 0
            if rsi is not None:
                if rsi < 20: score += 2
                elif rsi < 35: score += 1
                elif rsi > 80: score -= 2
                elif rsi > 65: score -= 1
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

        def format_action(score: int) -> Text:
            if score >= 6: return Text("STRONG BUY", style="bold green")
            if score >= 3: return Text("BUY", style="green")
            if score >= 1: return Text("LEAN BUY", style="green")
            if score <= -6: return Text("STRONG SELL", style="bold red")
            if score <= -3: return Text("SELL", style="red")
            if score <= -1: return Text("LEAN SELL", style="red")
            return Text("HOLD", style="dim white")

        def format_targets(ti: dict, score: int, price: Optional[float]) -> Text:
            if price is None:
                return Text("—", style="dim")
            fib_127, fib_161 = ti.get("fib_127"), ti.get("fib_161")
            fib_50, fib_618 = ti.get("fib_50"), ti.get("fib_618")

            def pfmt(v):
                return f"{v:.2f}"

            def confluences(tgt, sources):
                c = 0
                for v in sources:
                    if v and abs(tgt - v) / tgt < 0.003:
                        c += 1
                return c

            def pick_target(cands, is_buy, all_fibs):
                if not cands:
                    return None
                if is_buy:
                    stop_pool = [v for v in [fib_618, fib_50] if v and v < price]
                    stop = max(stop_pool) if stop_pool else price * 0.97
                else:
                    stop_pool = [v for v in [fib_127, fib_161] if v and v > price]
                    stop = min(stop_pool) if stop_pool else price * 1.03
                scored = []
                for lbl, val in cands:
                    risk = (price - stop) if is_buy else (stop - price)
                    reward = (val - price) if is_buy else (price - val)
                    if risk <= 0 or reward <= 0:
                        continue
                    rr = reward / risk
                    con = confluences(val, all_fibs)
                    scored.append((rr * (1 + 0.4 * con), val, con, lbl, rr))
                if not scored:
                    return None
                scored.sort(key=lambda x: x[0], reverse=True)
                return scored[0]

            fib_srcs = [v for v in [fib_50, fib_618, fib_127, fib_161] if v]

            if score >= 2:
                bc = [(l, v) for l, v in [("F50", fib_50), ("F618", fib_618),
                     ("F127", fib_127), ("F161", fib_161)] if v and v > price]
                best = pick_target(bc, True, fib_srcs)
                if best:
                    _, tgt, con, lbl, rr = best
                    bolts = "⚡" * min(con, 4)
                    return Text(f"↗{lbl} {pfmt(tgt)} {bolts} 1:{rr:.1f}", style="bold green" if rr >= 2 and con >= 2 else "green")

            if score <= -2:
                sc = [(l, v) for l, v in [("F127", fib_127), ("F161", fib_161),
                     ("F50", fib_50), ("F618", fib_618)] if v and v < price]
                best = pick_target(sc, False, fib_srcs)
                if best:
                    _, tgt, con, lbl, rr = best
                    bolts = "⚡" * min(con, 4)
                    return Text(f"↘{lbl} {pfmt(tgt)} {bolts} 1:{rr:.1f}", style="bold red")

            near_s = max([v for v in [fib_618, fib_50] if v and v < price], default=None)
            near_r = min([v for v in [fib_127, fib_161] if v and v > price], default=None)
            if near_s and near_r:
                return Text(f"↕{pfmt(near_s)} {pfmt(near_r)}", style="dim white")
            return Text("—", style="dim")

        for idx, tick in enumerate(rows):
            raw_sym = tick.get("symbol", "???")
            sym = raw_sym
            pr = tick.get("last_price")
            pct = tick.get("price_change_pct") or 0.0
            vol = tick.get("volume")
            high = tick.get("high_24h")
            low = tick.get("low_24h")
            ts = tick.get("timestamp") or "──:──:──"
            bid = tick.get("bid_price")
            ask = tick.get("ask_price")
            bid_qty = tick.get("bid_qty")
            ask_qty = tick.get("ask_qty")

            rsi_info = rsi_data.get(raw_sym, {})
            rsi_val = rsi_info.get("rsi_5")
            rsi_text = format_rsi(rsi_val)

            trend_text = Text("UP", style="bold green") if rsi_val is not None and rsi_val > 50 else Text("DOWN", style="bold red") if rsi_val is not None and rsi_val < 50 else Text("—", style="dim")

            pct_text = Text(
                f"{pct:+.2f}%", style="bold green" if pct >= 0 else "bold red"
            )

            ti = tech_data.get(raw_sym, {})
            pr_float = float(pr) if pr is not None else None
            ma20_val = ti.get("sma20")
            ma50_val = ti.get("sma50")
            ma200_val = ti.get("sma200")
            vwap_val = ti.get("vwap")
            atr_val = ti.get("atr")
            avg_vol_val = ti.get("avg_vol")
            bb_u = ti.get("bb_upper")
            bb_l = ti.get("bb_lower")
            fib_h = ti.get("fib_high")
            fib_low_val = ti.get("fib_low")

            ma20d = (pr_float - ma20_val) / ma20_val * 100 if pr_float is not None and ma20_val and ma20_val != 0 else None
            ma50d = (pr_float - ma50_val) / ma50_val * 100 if pr_float is not None and ma50_val and ma50_val != 0 else None
            ma200d = (pr_float - ma200_val) / ma200_val * 100 if pr_float is not None and ma200_val and ma200_val != 0 else None
            bbp = (pr_float - bb_l) / (bb_u - bb_l) * 100 if pr_float is not None and bb_u is not None and bb_l is not None and bb_u != bb_l else None
            fibp = (pr_float - fib_low_val) / (fib_h - fib_low_val) * 100 if pr_float is not None and fib_h is not None and fib_low_val is not None and fib_h != fib_low_val else None
            vwapd = (pr_float - vwap_val) / vwap_val * 100 if pr_float is not None and vwap_val and vwap_val != 0 else None
            rvol_ratio = vol / avg_vol_val if vol is not None and avg_vol_val and avg_vol_val != 0 else None

            sig_score = score_signal(rsi_val, bbp, fibp, ma20d, ma50d, ma200d, vwapd, rvol_ratio)
            act_text = format_action(sig_score)
            tgt_text = format_targets(ti, sig_score, pr_float)

            spread_text = format_spread(bid, ask, pr_float)
            ob_text = format_ob(pr_float, ti, bid, bid_qty, ask, ask_qty)
            rvol_text = format_rvol(vol, avg_vol_val)
            whales_text = format_whales(ti, bid_qty, ask_qty)
            vwap_text = format_vwap(pr_float, vwap_val)
            ma20_text = format_ma_dist(pr_float, ma20_val)
            ma50_text = format_ma_dist(pr_float, ma50_val)
            ma200_text = format_ma_dist(pr_float, ma200_val)
            bb_text = format_bb(pr_float, bb_u, bb_l)
            atr_text = format_atr(atr_val, pr_float)
            macd_text = format_macd_hist(ti.get("macd_histogram"))
            div_text = format_divergence(ti.get("macd_divergence"))
            fib_text = format_fib(pr_float, fib_low_val, fib_h)

            table.add_row(
                str(idx + 1),
                sym,
                act_text,
                tgt_text,
                trend_text,
                utils.format_price(pr, 4),
                pct_text,
                spread_text,
                ob_text,
                utils.format_volume(vol),
                rvol_text,
                whales_text,
                vwap_text,
                ma20_text,
                ma50_text,
                ma200_text,
                bb_text,
                atr_text,
                macd_text,
                div_text,
                fib_text,
                rsi_text,
                utils.format_price(high, 2),
                utils.format_price(low, 2),
                ts,
            )

        return table

    def _build_footer(self, tracked_count: int) -> Panel:
        """Builds the bottom status and help bar."""
        uptime_s = int(time.monotonic() - self.start_t)
        uptime = (
            f"{uptime_s // 3600:02d}:{(uptime_s % 3600) // 60:02d}:{uptime_s % 60:02d}"
        )

        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right", ratio=1)

        grid.add_row(
            Text.assemble(
                ("Tracking: ", "dim"), (f"{tracked_count} pairs", "bold white")
            ),
            Text.assemble(
                ("Log: ", "dim"), (os.path.basename(config.LOG_FILE), "cyan")
            ),
            Text.assemble(
                ("Uptime: ", "dim"),
                (f"{uptime}  ", "bold"),
                ("Press Ctrl-C to Stop", "dim italic"),
            ),
        )
        return Panel(grid, style="dim")


# ═══════════════════════════════════════════════════════════════════════════════
#  Tasks & Main Orchestration
# ═══════════════════════════════════════════════════════════════════════════════


async def display_loop(
    store: MarketDataStore,
    ws_client: BinanceWebSocketClient,
    rsi_store: RSIKlinesStore,
    tech_store: TechnicalIndicatorStore,
    shutdown_event: asyncio.Event,
) -> None:
    """Powers the Rich Live display engine."""
    log.info("Display loop started")
    ui = ScannerUI()

    # Create the Rich Live context to manage terminal redraws seamlessly
    with Live(
        ui.layout, refresh_per_second=10, screen=True, console=ui.console
    ) as live:
        while not shutdown_event.is_set():
            try:
                snapshot = await store.get_snapshot()
                rsi_data = await compute_all_rsi_realtime(snapshot, rsi_store)
                tech_data = await tech_store.get_all()
                live.update(ui.update(snapshot, ws_client.is_connected, rsi_data, tech_data))
            except Exception as exc:
                log.error("Display error: %s", exc)

            try:
                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=config.DISPLAY_REFRESH_INTERVAL,
                )
            except asyncio.TimeoutError:
                pass

    log.info("Display loop stopped")


async def fetch_historical_closes(symbol: str, interval: str = "1h", limit: int = 100) -> List[float]:
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    loop = asyncio.get_running_loop()
    def _fetch():
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        return [float(candle[4]) for candle in data]
    return await loop.run_in_executor(None, _fetch)


async def rsi_klines_task(symbols: List[str], rsi_store: RSIKlinesStore, shutdown_event: asyncio.Event) -> None:
    """Fetches historical klines periodically to maintain the 1-hour RSI."""
    while not shutdown_event.is_set():
        for sym in symbols:
            if shutdown_event.is_set():
                break
            try:
                closes = await fetch_historical_closes(sym, interval="1h", limit=100)
                await rsi_store.update(sym, closes)
            except Exception as e:
                log.error("Failed fetching 1h klines for %s: %s", sym, e)
            await asyncio.sleep(0.5) # Anti rate-limit jitter

        try:
            # Refresh every 60 seconds to catch candle rollovers
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
        except asyncio.TimeoutError:
            pass


async def dataframe_snapshot_task(
    store: MarketDataStore,
    df_manager: DataFrameManager,
    shutdown_event: asyncio.Event,
    interval: float = 5.0,
) -> None:
    """Periodically appends current tick data to the rolling DataFrame."""
    while not shutdown_event.is_set():
        try:
            snapshot = await store.get_snapshot()
            await df_manager.append_snapshot(snapshot)
        except Exception as exc:
            log.error("DataFrame snapshot error: %s", exc)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def yahoo_poll_task(
    symbols: List[str],
    store: MarketDataStore,
    shutdown_event: asyncio.Event,
    interval: float = 5.0,
) -> None:
    """Periodically fetch current price from Yahoo Finance and update MarketDataStore."""
    log.info("Yahoo poll task started for %s", symbols)
    while not shutdown_event.is_set():
        for sym in symbols:
            if shutdown_event.is_set():
                break
            try:
                data = await asyncio.get_running_loop().run_in_executor(
                    None, yahoo_finance.fetch_current, sym
                )
                if data:
                    tick: TickData = {
                        "symbol": sym,
                        "last_price": data["price"],
                        "price_change_pct": data["change_pct"],
                        "volume": data["volume"],
                        "high_24h": data["high"],
                        "low_24h": data["low"],
                        "bid_price": None,
                        "ask_price": None,
                        "bid_qty": None,
                        "ask_qty": None,
                        "timestamp": datetime.now(tz=timezone.utc).strftime("%H:%M:%S"),
                    }
                    await store.update(sym, tick)
            except Exception as e:
                log.debug("Yahoo poll error for %s: %s", sym, e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
    log.info("Yahoo poll task stopped")


async def yahoo_indicators_task(
    symbols: List[str],
    store: MarketDataStore,
    rsi_store: RSIKlinesStore,
    tech_store: TechnicalIndicatorStore,
    shutdown_event: asyncio.Event,
) -> None:
    """Periodically fetch Yahoo historical data and compute technical indicators."""
    log.info("Yahoo indicators task started for %s", symbols)
    while not shutdown_event.is_set():
        for sym in symbols:
            if shutdown_event.is_set():
                break
            try:
                klines = await asyncio.get_running_loop().run_in_executor(
                    None, yahoo_finance.fetch_ohlcv, sym, "1h", "3mo"
                )
                if not klines:
                    continue

                closes = [float(k[4]) for k in klines]
                await rsi_store.update(sym, closes)

                ind = compute_indicators_from_klines(klines)
                await tech_store.update(sym, ind)
            except Exception as e:
                log.debug("Yahoo indicators error for %s: %s", sym, e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=120.0)
        except asyncio.TimeoutError:
            pass
    log.info("Yahoo indicators task stopped")


def _install_signal_handlers(
    shutdown_event: asyncio.Event, loop: asyncio.AbstractEventLoop
) -> None:
    """Register OS signals for clean shutdown."""

    def _on_signal(signum, frame):
        log.warning("Signal %s received – shutting down…", signal.Signals(signum).name)
        loop.call_soon_threadsafe(shutdown_event.set)

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)


async def main() -> None:
    # 1. Discover symbols
    print("⏳ Fetching top symbols and connecting to Binance WebSocket...")
    symbols = await fetch_top_symbols(config.TOP_N_SYMBOLS + 30)
    symbols = [s for s in symbols if not any(stable in s.upper() for stable in config.STABLECOINS)]
    symbols = symbols[:config.TOP_N_SYMBOLS]
    symbols = [s for s in symbols if s != "XMRUSDT"]
    symbols = [s for s in symbols if re.match(r'^[A-Z0-9]{2,20}USDT$', s)]
    if "XAUTUSDT" not in symbols:
        symbols.append("XAUTUSDT")
    log.info("Top %d symbols (excl. stablecoins): %s", len(symbols), symbols)

    extra_symbols = ["GC=F"]
    all_symbols = symbols + extra_symbols
    log.info("Extra non-Binance symbols: %s", extra_symbols)

    # 2. Init Core logic
    store = MarketDataStore(all_symbols)
    df_manager = DataFrameManager()
    rsi_store = RSIKlinesStore()
    tech_store = TechnicalIndicatorStore()

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    _install_signal_handlers(shutdown_event, loop)

    ws_client = BinanceWebSocketClient(
        symbols=symbols,
        store=store,
        df_manager=df_manager,
        on_tick=None,
        shutdown_event=shutdown_event,
    )

    # 3. Launch background tasks
    tasks = [
        asyncio.create_task(ws_client.run(), name="ws_client"),
        asyncio.create_task(
            rsi_klines_task(symbols, rsi_store, shutdown_event), name="rsi_klines"
        ),
        asyncio.create_task(
            display_loop(store, ws_client, rsi_store, tech_store, shutdown_event), name="display_loop"
        ),
        asyncio.create_task(
            dataframe_snapshot_task(store, df_manager, shutdown_event),
            name="df_snapshot",
        ),
        asyncio.create_task(
            indicators_update_task(symbols, tech_store, shutdown_event),
            name="tech_indicators",
        ),
        asyncio.create_task(
            yahoo_poll_task(extra_symbols, store, shutdown_event),
            name="yahoo_poll",
        ),
        asyncio.create_task(
            yahoo_indicators_task(extra_symbols, store, rsi_store, tech_store, shutdown_event),
            name="yahoo_indicators",
        ),
    ]

    # Wait for exit signal
    await shutdown_event.wait()

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    # 4. Clean exit
    try:
        export_path = os.path.join(config.LOG_DIR, "final_snapshot.csv")
        await df_manager.export_csv(export_path)
        print(f"\n✅ Snapshot successfully saved → {export_path}")
    except Exception as exc:
        log.error("Export failed: %s", exc)

    print("👋 Scanner stopped gracefully.\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    sys.exit(0)
