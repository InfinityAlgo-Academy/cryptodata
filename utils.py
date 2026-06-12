"""
utils.py - Shared helper utilities for the Real-Time Crypto Market Scanner
===========================================================================
Functions here are intentionally stateless and free of side-effects so they
can be reused by any future module (signals, AI, Telegram, etc.).
"""

import logging
import logging.handlers
import os
from datetime import datetime, timezone
from typing import Optional

import config

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------


def setup_logging() -> logging.Logger:
    """
    Configure and return the root application logger.

    Behaviour
    ---------
    * Rotating file handler ONLY – all levels go to the log file.
    * NO console handler – prevents log messages from polluting the live
      terminal display. The display loop owns stdout exclusively.
    * Log directory is created automatically if it does not exist.
    """
    os.makedirs(config.LOG_DIR, exist_ok=True)

    numeric_level = getattr(logging, config.LOG_LEVEL.upper(), logging.DEBUG)

    logger = logging.getLogger("crypto_scanner")
    logger.setLevel(numeric_level)

    # Guard against duplicate handlers when the function is called more than once
    if logger.handlers:
        return logger

    # ----- Formatter definition -----
    verbose_fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ----- Rotating file handler (DEBUG+) – only output destination -----
    file_handler = logging.handlers.RotatingFileHandler(
        filename=config.LOG_FILE,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(verbose_fmt)

    logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the application namespace."""
    return logging.getLogger(f"crypto_scanner.{name}")


# ---------------------------------------------------------------------------
# Data-formatting helpers
# ---------------------------------------------------------------------------


def format_price(value: Optional[float], decimals: int = config.PRICE_DECIMALS) -> str:
    """
    Format a numeric price for display.

    Parameters
    ----------
    value    : float or None – the price to format.
    decimals : int           – number of decimal places.

    Returns
    -------
    str – formatted price string, e.g. "42,350.1234", or "N/A" if value is None.
    """
    if value is None:
        return "N/A"
    return f"{value:,.{decimals}f}"


def format_pct(
    value: Optional[float], decimals: int = config.PCT_CHANGE_DECIMALS
) -> str:
    """
    Format a percentage value with a sign prefix and colour indicator string.

    Returns
    -------
    str – e.g. "+3.21%" or "-1.05%", or "N/A".
    """
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{decimals}f}%"


def format_volume(value: Optional[float]) -> str:
    """
    Format a large volume number into a human-readable abbreviated string.

    Examples
    --------
    1_500_000  → "1.50M"
    23_400     → "23.40K"
    850        → "850.00"
    """
    if value is None:
        return "N/A"
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


def utc_now_str() -> str:
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def pct_color_code(value: Optional[float]) -> str:
    """
    Return an ANSI escape code for the given percentage change.

    Positive → green, Negative → red, Zero / None → reset.
    """
    if value is None or value == 0:
        return "\033[0m"  # reset
    return "\033[92m" if value > 0 else "\033[91m"


ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_CYAN = "\033[96m"
ANSI_YELLOW = "\033[93m"
ANSI_GREEN = "\033[92m"
ANSI_RED = "\033[91m"
ANSI_WHITE = "\033[97m"
ANSI_DIM = "\033[2m"


def colored(text: str, code: str) -> str:
    """Wrap *text* in an ANSI escape *code* and reset afterwards."""
    return f"{code}{text}{ANSI_RESET}"


# ---------------------------------------------------------------------------
# Exponential back-off helper
# ---------------------------------------------------------------------------


def compute_backoff(attempt: int) -> float:
    """
    Compute the back-off delay for a given reconnection *attempt* number.

    Parameters
    ----------
    attempt : int – zero-based attempt index.

    Returns
    -------
    float – delay in seconds, capped at config.RECONNECT_MAX_DELAY.
    """
    delay = config.RECONNECT_BASE_DELAY * (config.RECONNECT_BACKOFF_FACTOR**attempt)
    return min(delay, config.RECONNECT_MAX_DELAY)
