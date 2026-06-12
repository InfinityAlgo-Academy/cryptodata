"""
websocket_client.py - Binance WebSocket client with auto-reconnect
===================================================================
Responsibilities
----------------
1. Open a single combined WebSocket connection to Binance for all tracked
   symbols (one connection = one TCP handshake = minimal overhead).
2. Dispatch parsed tick messages to the MarketDataStore and DataFrameManager.
3. Implement exponential back-off reconnection so the scanner survives
   network glitches, server-side resets, and Binance maintenance windows.
4. Expose a clean shutdown mechanism via asyncio.Event.

Extensibility hooks
-------------------
The ``on_tick`` callback is injected at construction time.  Future modules
(trading signals, Telegram alerts, AI predictions) can wrap or replace it
without touching this file.
"""

import asyncio
import json
from typing import Callable, Coroutine, List, Optional

import websockets
import websockets.exceptions

import config
from market_data import (
    MarketDataStore,
    DataFrameManager,
    parse_ticker_message,
    TickData,
)
from utils import get_logger, compute_backoff

logger = get_logger("websocket_client")

# Type alias for the optional user-supplied tick callback
TickCallback = Callable[[TickData], Coroutine]


# ---------------------------------------------------------------------------
# Symbol discovery via Binance REST
# ---------------------------------------------------------------------------


async def fetch_top_symbols(n: int = config.TOP_N_SYMBOLS) -> List[str]:
    """
    Fetch the top *n* USDT-quoted symbols.

    Strategy: pull top coins by market cap from CoinGecko, cross-reference
    against Binance USDT pairs, then pick the most liquid (highest 24h volume)
    among the matches.

    Parameters
    ----------
    n : int – number of top symbols to return.

    Returns
    -------
    list[str] – symbol strings, e.g. ["BTCUSDT", "ETHUSDT", ...]
    """
    import urllib.request
    import re

    VALID_SYMBOL = re.compile(r'^[A-Z0-9]{2,20}USDT$')

    logger.info("Fetching top %d symbols by CoinGecko market cap + Binance volume…", n)
    try:
        # 1. Get available Binance USDT pairs with 24h volume
        with urllib.request.urlopen(config.BINANCE_REST_TICKER_URL, timeout=10) as resp:
            binance_tickers = json.loads(resp.read().decode())

        binance_by_base = {}
        binance_volume = {}
        for t in binance_tickers:
            sym = t["symbol"]
            if sym.endswith(config.QUOTE_ASSET) and not sym.startswith(config.QUOTE_ASSET):
                base = sym.replace(config.QUOTE_ASSET, "").upper()
                binance_by_base[base] = sym
                binance_volume[sym] = float(t.get("quoteVolume", 0))

        # 2. Fetch top coins by market cap from CoinGecko
        cg_url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&sparkline=false"
        with urllib.request.urlopen(cg_url, timeout=15) as resp:
            cg_data = json.loads(resp.read().decode())

        # 3. Cross-reference & collect matching Binance pairs
        matched = []
        for coin in cg_data:
            base = coin["symbol"].upper()
            if base in binance_by_base:
                matched.append(binance_by_base[base])

        # 4. Remove any non-ASCII / corrupted symbols
        matched = [s for s in matched if VALID_SYMBOL.match(s)]

        if not matched:
            raise Exception("No matching Binance pairs found from CoinGecko data")

        # 4. Sort by 24h Binance quote volume descending, take top n
        matched.sort(key=lambda s: binance_volume.get(s, 0), reverse=True)
        result = matched[:n]

        logger.info("Top %d symbols (volume-sorted): %s", len(result), result)
        return result

    except Exception as exc:
        logger.error("Failed to fetch by market cap: %s", exc)
        logger.info("Falling back to Binance volume-sorted symbols…")
        try:
            with urllib.request.urlopen(config.BINANCE_REST_TICKER_URL, timeout=10) as resp:
                tickers = json.loads(resp.read().decode())
            usdt_tickers = [
                t for t in tickers
                if t["symbol"].endswith(config.QUOTE_ASSET)
                and not t["symbol"].startswith(config.QUOTE_ASSET)
            ]
            usdt_tickers.sort(key=lambda t: float(t.get("quoteVolume", 0)), reverse=True)
            result = [t["symbol"] for t in usdt_tickers[:n * 2]]
            result = [s for s in result if VALID_SYMBOL.match(s)]
            return result[:n]
        except Exception:
            fallback = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
                        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "SHIBUSDT", "DOTUSDT"]
            logger.warning("Using fallback symbol list: %s", fallback)
            return fallback[:n]


# ---------------------------------------------------------------------------
# BinanceWebSocketClient
# ---------------------------------------------------------------------------


class BinanceWebSocketClient:
    """
    Async WebSocket client that subscribes to Binance combined ticker streams.

    Parameters
    ----------
    symbols        : list[str]         – symbols to track (e.g. ["BTCUSDT", ...]).
    store          : MarketDataStore   – receives parsed tick updates.
    df_manager     : DataFrameManager  – accumulates snapshot history.
    on_tick        : TickCallback|None – optional async callback fired for each tick.
    shutdown_event : asyncio.Event     – set this event to stop the client cleanly.
    """

    def __init__(
        self,
        symbols: List[str],
        store: MarketDataStore,
        df_manager: DataFrameManager,
        on_tick: Optional[TickCallback] = None,
        shutdown_event: Optional[asyncio.Event] = None,
    ) -> None:
        self.symbols = symbols
        self.store = store
        self.df_manager = df_manager
        self.on_tick = on_tick
        self.shutdown_event = shutdown_event or asyncio.Event()

        # Build the Binance combined stream URL
        # Pattern: wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker
        streams = "/".join(f"{s.lower()}@ticker" for s in symbols)
        self._ws_url = f"{config.BINANCE_COMBINED_STREAM_URL}?streams={streams}"

        self._attempt: int = 0  # consecutive reconnect counter
        self._connected: bool = False

        logger.info("BinanceWebSocketClient created for %d symbols", len(symbols))
        logger.debug("WebSocket URL: %s", self._ws_url)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Start the WebSocket loop.  Runs until ``shutdown_event`` is set or
        ``MAX_RECONNECT_ATTEMPTS`` is exceeded (0 means infinite retries).
        """
        while not self.shutdown_event.is_set():
            try:
                await self._connect_and_listen()
                # If we reach here the connection closed cleanly – reconnect anyway
                self._attempt = 0
            except asyncio.CancelledError:
                logger.info("WebSocket task cancelled – shutting down")
                break
            except Exception as exc:
                self._attempt += 1
                if (
                    config.MAX_RECONNECT_ATTEMPTS > 0
                    and self._attempt > config.MAX_RECONNECT_ATTEMPTS
                ):
                    logger.critical(
                        "Max reconnect attempts (%d) reached. Giving up.",
                        config.MAX_RECONNECT_ATTEMPTS,
                    )
                    self.shutdown_event.set()
                    break

                delay = compute_backoff(self._attempt - 1)
                logger.warning(
                    "Connection lost (attempt %d): %s — retrying in %.1fs",
                    self._attempt,
                    exc,
                    delay,
                )
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass  # Back-off period elapsed, proceed with reconnect

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _connect_and_listen(self) -> None:
        """
        Open the WebSocket, reset the attempt counter on success, then
        forward each message to the processing pipeline.
        """
        logger.info("Connecting to Binance WebSocket… (attempt %d)", self._attempt + 1)

        async with websockets.connect(
            self._ws_url,
            ping_interval=20,  # keep-alive ping every 20 s
            ping_timeout=10,  # fail if pong not received within 10 s
            close_timeout=5,
            max_size=2**20,  # 1 MB max message size
        ) as ws:
            self._connected = True
            self._attempt = 0  # reset on successful connection
            logger.info("✅ WebSocket connected successfully")

            async for raw_message in ws:
                if self.shutdown_event.is_set():
                    break
                await self._handle_message(raw_message)

        self._connected = False
        logger.info("WebSocket connection closed")

    # ------------------------------------------------------------------
    async def _handle_message(self, raw: str) -> None:
        """
        Decode a raw WebSocket message and route it to the data store.

        Parameters
        ----------
        raw : str – raw JSON string from Binance.
        """
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("JSON decode error: %s | raw=%s", exc, raw[:200])
            return

        tick = parse_ticker_message(payload)
        if tick is None:
            return  # Not a ticker message (e.g. subscription confirmation)

        symbol = tick["symbol"]

        # ---- Update the live data store ----
        await self.store.update(symbol, tick)

        # ---- Append to rolling DataFrame (non-blocking snapshot) ----
        await self.df_manager.append_snapshot([tick])

        # ---- Fire optional user callback ----
        if self.on_tick is not None:
            try:
                await self.on_tick(tick)
            except Exception as exc:
                logger.error("on_tick callback raised an exception: %s", exc)

    # ------------------------------------------------------------------
    @property
    def is_connected(self) -> bool:
        """True if a WebSocket connection is currently active."""
        return self._connected
