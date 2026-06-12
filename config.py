"""
config.py - Central configuration for the Real-Time Crypto Market Scanner
===========================================================================
All tunable parameters, endpoints, and feature flags live here.
Edit this file to adapt the scanner to your needs without touching core logic.
"""

import os

# ---------------------------------------------------------------------------
# Binance WebSocket endpoints
# ---------------------------------------------------------------------------
BINANCE_WS_BASE_URL = "wss://stream.binance.com:9443"

# Combined stream endpoint – subscribe to multiple streams in one connection
BINANCE_COMBINED_STREAM_URL = f"{BINANCE_WS_BASE_URL}/stream"

# REST endpoint used to discover top symbols by volume (24-h ticker)
BINANCE_REST_TICKER_URL = "https://api.binance.com/api/v3/ticker/24hr"

# ---------------------------------------------------------------------------
# Symbol discovery
# ---------------------------------------------------------------------------
# Quote asset used to filter trading pairs (USDT pairs only)
QUOTE_ASSET = "USDT"

# Number of top symbols (by quote volume) to track
TOP_N_SYMBOLS = 50

# Symbols to exclude (stablecoins, etc.)
STABLECOINS = ["USDC", "USDD", "USD1", "USDS", "USDE", "DAI", "BUSD", "TUSD", "FRAX", "USTC", "PAX", "GUSD", "LUSD", "FDUSD"]

# ---------------------------------------------------------------------------
# WebSocket reconnection settings
# ---------------------------------------------------------------------------
# Maximum number of consecutive reconnection attempts before giving up (0 = infinite)
MAX_RECONNECT_ATTEMPTS = 0

# Initial back-off delay (seconds) before the first reconnect attempt
RECONNECT_BASE_DELAY = 1.0

# Maximum back-off delay (seconds) – caps the exponential growth
RECONNECT_MAX_DELAY = 60.0

# Multiplier applied to back-off delay after each failed attempt
RECONNECT_BACKOFF_FACTOR = 2.0

# ---------------------------------------------------------------------------
# Data / DataFrame settings
# ---------------------------------------------------------------------------
# Maximum number of rows kept in the historical DataFrame (memory guard)
MAX_DATAFRAME_ROWS = 10_000

# ---------------------------------------------------------------------------
# Terminal display settings
# ---------------------------------------------------------------------------
# Refresh rate of the live terminal table (seconds between redraws)
DISPLAY_REFRESH_INTERVAL = 1.0

# Number of decimal places shown for prices
PRICE_DECIMALS = 4

# Number of decimal places shown for percentage change
PCT_CHANGE_DECIMALS = 2

# ---------------------------------------------------------------------------
# Logging settings
# ---------------------------------------------------------------------------
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE = os.path.join(LOG_DIR, "crypto_scanner.log")

# Log rotation: max file size (bytes) before a new file is created
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB

# Number of backup log files to keep
LOG_BACKUP_COUNT = 5

# Logging level: DEBUG | INFO | WARNING | ERROR | CRITICAL
LOG_LEVEL = "DEBUG"

# ---------------------------------------------------------------------------
# Future feature flags (set True to enable when implemented)
# ---------------------------------------------------------------------------
ENABLE_TRADING_SIGNALS = False  # Elliott Wave / custom signal engine
ENABLE_ELLIOTT_WAVE = False  # Elliott Wave pattern detection
ENABLE_FIBONACCI = False  # Fibonacci retracement / extension levels
ENABLE_TELEGRAM_BOT = False  # Forward alerts to a Telegram bot
ENABLE_AI_PREDICTIONS = False  # ML-based price direction predictions

# Telegram credentials (required when ENABLE_TELEGRAM_BOT = True)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ---------------------------------------------------------------------------
# DataFrame column definitions (single source of truth)
# ---------------------------------------------------------------------------
DATAFRAME_COLUMNS = [
    "symbol",
    "last_price",
    "price_change_pct",
    "volume",
    "high_24h",
    "low_24h",
    "bid_price",
    "ask_price",
    "bid_qty",
    "ask_qty",
    "timestamp",
]
