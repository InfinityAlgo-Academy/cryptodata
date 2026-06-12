"""
market_data.py - Thread-safe market data store and DataFrame manager
=====================================================================
This module owns the canonical, in-memory state of all tracked symbols.
It exposes:

  * MarketDataStore  – a thread-safe dict-like container updated by the
                       WebSocket client and read by the display layer.
  * DataFrameManager – wraps a pandas DataFrame and keeps it bounded by
                       config.MAX_DATAFRAME_ROWS to guard memory usage.

Design notes
------------
* asyncio.Lock is used because updates arrive inside async coroutines.
* The DataFrame is stored as an attribute so callers can take snapshots
  for analytics (signals, AI, Fibonacci, Elliott Wave) without blocking
  the WebSocket loop.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional, List

import pandas as pd

import config
from utils import get_logger

logger = get_logger("market_data")


# ---------------------------------------------------------------------------
# Tick data model (plain dict schema – avoids heavy dataclass overhead)
# ---------------------------------------------------------------------------
# Keys mirror config.DATAFRAME_COLUMNS for easy DataFrame construction.
TickData = Dict[str, Optional[object]]


def empty_tick(symbol: str) -> TickData:
    """Return a zeroed-out tick record for *symbol*."""
    return {
        "symbol": symbol,
        "last_price": None,
        "price_change_pct": None,
        "volume": None,
        "high_24h": None,
        "low_24h": None,
        "bid_price": None,
        "ask_price": None,
        "bid_qty": None,
        "ask_qty": None,
        "timestamp": None,
    }


# ---------------------------------------------------------------------------
# MarketDataStore
# ---------------------------------------------------------------------------


class MarketDataStore:
    """
    Thread-safe, async-compatible in-memory store for the latest tick data.

    Usage
    -----
    store = MarketDataStore(symbols)
    await store.update("BTCUSDT", {...})
    snapshot = await store.get_snapshot()
    """

    def __init__(self, symbols: List[str]) -> None:
        """
        Parameters
        ----------
        symbols : list[str] – initial list of symbols to track.
        """
        self._lock: asyncio.Lock = asyncio.Lock()
        # Pre-populate with empty records so the display always has rows
        self._data: Dict[str, TickData] = {s: empty_tick(s) for s in symbols}
        logger.info("MarketDataStore initialised with %d symbols", len(symbols))

    # ------------------------------------------------------------------
    async def update(self, symbol: str, tick: TickData) -> None:
        """
        Atomically replace the stored record for *symbol* with *tick*.

        Parameters
        ----------
        symbol : str      – e.g. "BTCUSDT"
        tick   : TickData – freshly parsed WebSocket message.
        """
        async with self._lock:
            self._data[symbol] = tick
        logger.debug("Updated %s @ %.4f", symbol, tick.get("last_price") or 0)

    # ------------------------------------------------------------------
    async def get_snapshot(self) -> List[TickData]:
        """
        Return a deep-copy snapshot of all current tick records.

        Returns
        -------
        list[TickData] – one entry per tracked symbol.
        """
        async with self._lock:
            return [dict(record) for record in self._data.values()]

    # ------------------------------------------------------------------
    async def get_tick(self, symbol: str) -> Optional[TickData]:
        """Return the latest tick for a single *symbol*, or None."""
        async with self._lock:
            return dict(self._data[symbol]) if symbol in self._data else None

    # ------------------------------------------------------------------
    @property
    def symbols(self) -> List[str]:
        """Return the list of tracked symbols (order not guaranteed)."""
        return list(self._data.keys())


# ---------------------------------------------------------------------------
# DataFrameManager
# ---------------------------------------------------------------------------


class DataFrameManager:
    """
    Maintains a rolling pandas DataFrame of tick snapshots.

    Each call to ``append_snapshot`` adds one row per symbol and
    trims the DataFrame to stay within config.MAX_DATAFRAME_ROWS.
    This lets downstream analytics modules query historical ticks without
    holding the WebSocket lock.

    Thread / async safety
    ---------------------
    append_snapshot is synchronous (pandas is not async-aware) and should
    be called from a dedicated asyncio task or thread executor if needed.
    """

    def __init__(self) -> None:
        self._df: pd.DataFrame = pd.DataFrame(columns=config.DATAFRAME_COLUMNS)
        self._lock = asyncio.Lock()
        logger.info("DataFrameManager ready (max_rows=%d)", config.MAX_DATAFRAME_ROWS)

    # ------------------------------------------------------------------
    async def append_snapshot(self, snapshot: List[TickData]) -> None:
        """
        Append a list of tick records to the DataFrame.

        Parameters
        ----------
        snapshot : list[TickData] – output of MarketDataStore.get_snapshot().
        """
        if not snapshot:
            return

        new_rows = pd.DataFrame(snapshot, columns=config.DATAFRAME_COLUMNS)

        async with self._lock:
            self._df = pd.concat([self._df, new_rows], ignore_index=True)

            # ---- Memory guard: keep only the most recent N rows ----
            if len(self._df) > config.MAX_DATAFRAME_ROWS:
                excess = len(self._df) - config.MAX_DATAFRAME_ROWS
                self._df = self._df.iloc[excess:].reset_index(drop=True)
                logger.debug("DataFrame trimmed by %d rows", excess)

    # ------------------------------------------------------------------
    async def get_latest(self) -> pd.DataFrame:
        """
        Return the most recent tick for each tracked symbol.

        Returns
        -------
        pd.DataFrame – one row per symbol, most-recent record wins.
        """
        async with self._lock:
            if self._df.empty:
                return self._df.copy()
            # Keep the last occurrence of each symbol
            return (
                self._df.drop_duplicates(subset=["symbol"], keep="last")
                .reset_index(drop=True)
                .copy()
            )

    # ------------------------------------------------------------------
    async def get_full_history(self) -> pd.DataFrame:
        """Return a copy of the full rolling history DataFrame."""
        async with self._lock:
            return self._df.copy()

    # ------------------------------------------------------------------
    async def export_csv(self, path: str) -> None:
        """
        Export the current DataFrame to a CSV file.

        Parameters
        ----------
        path : str – destination file path.
        """
        async with self._lock:
            self._df.to_csv(path, index=False)
        logger.info("DataFrame exported to %s", path)


# ---------------------------------------------------------------------------
# Tick parser
# ---------------------------------------------------------------------------


def parse_ticker_message(raw: dict) -> Optional[TickData]:
    """
    Parse a Binance 24-h individual symbol ticker stream message.

    Binance stream name: <symbol>@ticker

    Parameters
    ----------
    raw : dict – decoded JSON payload from WebSocket.

    Returns
    -------
    TickData or None if the message is malformed.

    Binance field reference
    -----------------------
    e  – event type ("24hrTicker")
    s  – symbol
    c  – last price
    P  – price change percent
    q  – total traded quote asset volume (last 24 h)
    h  – high price (24 h)
    l  – low price (24 h)
    b  – best bid price
    a  – best ask price
    E  – event time (ms UTC)
    """
    try:
        # Combined stream wraps payload in {"stream": "...", "data": {...}}
        data = raw.get("data", raw)

        if data.get("e") != "24hrTicker":
            return None

        return {
            "symbol": data["s"],
            "last_price": float(data["c"]),
            "price_change_pct": float(data["P"]),
            "volume": float(data["q"]),  # quote volume
            "high_24h": float(data["h"]),
            "low_24h": float(data["l"]),
            "bid_price": float(data["b"]),
            "ask_price": float(data["a"]),
            "bid_qty": float(data["B"]),
            "ask_qty": float(data["A"]),
            "timestamp": datetime.fromtimestamp(
                int(data["E"]) / 1000,
                tz=timezone.utc,
            ).strftime("%H:%M:%S"),
        }
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Failed to parse ticker message: %s | raw=%s", exc, raw)
        return None
