<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://img.shields.io/badge/CryptoData-%23000000?style=for-the-badge&logo=bitcoin&logoColor=F7931A">
    <img src="https://img.shields.io/badge/CryptoData-%23000000?style=for-the-badge&logo=bitcoin&logoColor=F7931A" alt="CryptoData" width="300">
  </picture>
</p>

<p align="center">
  <b>Real-Time Binance Crypto Market Scanner</b><br>
  <i>Live terminal UI • Technical Indicators • Order-Book Walls • Composite Signals</i>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9%2B-3776AB?style=flat-square&logo=python&logoColor=white">
  <img src="https://img.shields.io/badge/Binance-API-F0B90B?style=flat-square&logo=binance&logoColor=white">
  <img src="https://img.shields.io/badge/Rich-TUI-3178C6?style=flat-square">
  <img src="https://img.shields.io/badge/WebSocket-Realtime-00BCD4?style=flat-square">
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square">
</p>

---

## Overview

**CryptoData** is a professional-grade, real-time cryptocurrency market scanner that streams live data from Binance directly into your terminal. It tracks the top 20 non-stablecoin USDT pairs by market cap and presents a rich, flicker-free dashboard with:

- **22 columns** of market data, technical indicators, and order-book analytics
- **Composite action signal** (STRONG BUY → STRONG SELL) from 8 weighted inputs
- **Order-book wall detection** — identifies the largest bid/ask clusters from depth snapshots
- **Smart target engine** — ranks 7+ candidate resistance/support levels by risk-reward × confluence
- **Fibonacci golden zone** — highlights 50%–61.8% retracement in green
- **Whale activity monitoring** — tracks dominant wall volumes in millions

---

## Demo

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ №  SYMBOL    ACT         TGT                   PRICE       CHG%    VOL     │
│ 1  BTCUSDT   STRONG BUY  ↗R1 64800 ⚡ 1:3.2    63520.50   +2.34%   1.2B    │
│ 2  ETHUSDT   BUY         ↗R1 3520 ⚡⚡ 1:2.8    3485.20    +1.87%   850M    │
│ 3  SOLUSDT   LEAN BUY    ↗R1 185.5 1:1.9        178.30    +0.92%   320M    │
│ 4  XRPUSDT   HOLD        ↕0.48 0.52               0.51    -0.15%   180M    │
│ 5  DOGEUSDT  SELL        ↘S1 0.14 ⚡ 1:2.1        0.15    -1.20%   95M     │
└─────────────────────────────────────────────────────────────────────────────┘
```

> *Note: Requires a real terminal. The Rich Live display will not render in non-interactive shells (pipelines, IDEs, etc.).*

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/InfinityAlgo-Academy/cryptodata.git
cd cryptodata

# Install dependencies
pip install -r requirements.txt

# Launch the scanner
python3 main.py
```

**Requirements:** Python 3.9+, a real terminal emulator (iTerm2, Warp, GNOME Terminal, Windows Terminal, etc.), and an active internet connection.

---

## Dashboard Columns

The terminal table is organized into logical groups for quick scanning.

### Price & Liquidity

| Column | Label | Description |
|--------|-------|-------------|
| № | — | Rank by market cap |
| SYMBOL | — | Binance trading pair (full name, e.g. `BTCUSDT`) |
| TICK | — | Tick direction relative to previous price (`↑` `↓` `→`) |
| PRICE | yellow | Last traded price |
| CHG% | green/red | 24-hour price change percentage |
| SPR | — | Bid-ask spread in basis points (<5 bps = green, >20 bps = red) |
| OB | — | **Order book walls:** largest bid/ask cluster price + volume in millions |
| VOL | magenta | 24-hour quote volume |
| R VOL | — | Relative volume vs 24-hour average (`>1.5x` = unusual activity) |
| WHALES | — | Dominant order-book wall: side (`B`/`S`) + volume in millions |

### Technical Indicators

| Column | Label | Description |
|--------|-------|-------------|
| VWAP | green/red | % distance from Volume-Weighted Average Price |
| MA20 | green/red | % distance from 20-period SMA |
| MA50 | green/red | % distance from 50-period SMA |
| MA200 | green/red | % distance from 200-period SMA |
| BB% | — | Bollinger Band % position (`<15%` = oversold green, `>85%` = overbought red) |
| ATR | — | Average True Range as % of price (volatility gauge) |
| FIB | — | Fibonacci retracement level (`50%–61.8%` = golden zone, green highlight) |
| RSI | green/red | 5-period Relative Strength Index |

### Signals & Targets

| Column | Label | Description |
|--------|-------|-------------|
| ACT | — | **Composite action signal** (7 levels: STRONG BUY → STRONG SELL) |
| TGT | — | **Best target:** label + price + confluence bolts (⚡) + risk-reward ratio |

### Reference

| Column | Label | Description |
|--------|-------|-------------|
| HIGH | white | 24-hour high |
| LOW | white | 24-hour low |
| TIME | dim | Last WebSocket update timestamp |

---

## Composite Action Signal

The **ACT** column condenses 8 technical inputs into a single score from **−14 to +14**, mapped to 7 action levels:

```
≥ 6  ── STRONG BUY    (bold green)
≥ 3  ── BUY            (green)
≥ 1  ── LEAN BUY       (green)
  0  ── HOLD           (dim white)
≤ −1 ── LEAN SELL      (red)
≤ −3 ── SELL            (red)
≤ −6 ── STRONG SELL    (bold red)
```

### Inputs & Weights

| Input | Bullish (score) | Bearish (score) |
|-------|----------------|-----------------|
| RSI5 | <20 (+2), <35 (+1) | >80 (−2), >65 (−1) |
| BB% | <10% (+2), <25% (+1) | >90% (−2), >75% (−1) |
| Fib% | <23.6% (+2), <50% (+1) | >78.6% (−2), >61.8% (−1) |
| MA20 dist | <−3% (+2), <−1% (+1) | >+3% (−2), >+1% (−1) |
| MA50 dist | <−5% (+2), <−2% (+1) | >+5% (−2), >+2% (−1) |
| MA200 dist | <−10% (+2), <−5% (+1) | >+10% (−2), >+5% (−1) |
| VWAP dist | <−1% (+1) | >+1% (−1) |
| R VOL | >1.5× (amplifies direction) | <0.5× (halves magnitude) |

---

## Smart Target Engine

The **TGT** column finds the single best price target using a multi-source scoring system.

### Sources

| Side | Sources | Labels |
|------|---------|--------|
| **Buy** (resistance above) | Pivot R1/R2, BB Upper, Fib 127.2%/161.8%, Ask Wall, VWAP, Round Number | `R1` `R2` `BBU` `F127` `F161` `AW` `VW` `RN` |
| **Sell** (support below) | Pivot S1/S2, BB Lower, Fib 50%/61.8%, Bid Wall, VWAP, Round Number | `S1` `S2` `BBL` `F50` `F618` `BW` `VW` `RN` |

### Scoring

Each candidate target is scored as:

```
score = RR × (1 + 0.4 × confluence)

where:
  RR         = reward ÷ risk (distance to target ÷ distance to stop)
  confluence = number of independent sources within 0.3% of the target
```

The stop is dynamically selected from the best available level across pivots, BB, VWAP, order-book walls, or a 3% fallback.

### Display

```
↗R1 64820.00 ⚡⚡ 1:3.2
└ label      └ confluences  └ RR ratio
```

- `bold green` when RR ≥ 2 **and** confluence ≥ 2
- `green` otherwise
- Confluence bolts (⚡) cap at 4

---

## Order-Book Wall Detection

The **OB** column displays the largest bid and ask clusters from a 100-level depth snapshot.

```
B 64820.00 15.24M│S 65100.50 22.10M
└ bid cluster     └ ask cluster
   price + volume    price + volume
```

### Algorithm

1. Fetch 100-level depth snapshot via Binance REST API
2. Group adjacent price levels within **0.2%** into clusters
3. Pick the cluster with the **highest total volume** per side (no threshold)
4. Fall back to top-of-book bid/ask quantities if depth API fails

The **WHALES** column shows the single larger of the two wall volumes with its side label.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        main.py                                  │
│  ┌────────────┐  ┌──────────────┐  ┌─────────────────────────┐ │
│  │  TUI Layout │  │  Signal      │  │  DataFrame Snapshot     │ │
│  │  (Rich Live)│  │  Scorer      │  │  (CSV Export)           │ │
│  └─────┬───────┘  └──────┬───────┘  └──────────┬──────────────┘ │
└────────┼──────────────────┼──────────────────────┼───────────────┘
         │                  │                      │
┌────────▼──────────────────▼──────────────────────▼───────────────┐
│                     websocket_client.py                          │
│  Binance Combined Stream (<symbol>@ticker) + Symbol Discovery   │
│                   (CoinGecko → Binance → fallback)               │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│                       market_data.py                              │
│  MarketDataStore (async thread-safe) + DataFrameManager          │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│                    technical_indicators.py                        │
│  SMA • BB • VWAP • Pivot R/S • Fib • ATR • Depth Walls          │
│  (Fetches klines + depth via REST, refreshes every 120s)         │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Binance WS ──► MarketDataStore ──► Rich TUI (1s refresh)
                 │
                 └──► TechnicalIndicatorStore ◄── REST klines + depth (120s)
                          │
                          └──► Score Signal → ACT / TGT columns
```

---

## Configuration

All parameters are centralized in `config.py`:

```python
# ── Symbol Discovery ──────────────────────────
TOP_N_SYMBOLS = 20        # Number of pairs to track
STABLECOINS = [...]       # Substrings to filter out
QUOTE_ASSET = "USDT"      # Quote currency

# ── WebSocket ──────────────────────────────────
MAX_RECONNECT_ATTEMPTS = 0    # 0 = infinite
RECONNECT_BASE_DELAY = 1.0    # Initial backoff (s)
RECONNECT_MAX_DELAY = 60.0    # Max backoff (s)
RECONNECT_BACKOFF_FACTOR = 2.0

# ── Display ────────────────────────────────────
DISPLAY_REFRESH_INTERVAL = 1.0    # TUI refresh (s)
PRICE_DECIMALS = 4

# ── Logging ────────────────────────────────────
LOG_LEVEL = "DEBUG"
LOG_MAX_BYTES = 10 * 1024 * 1024   # 10 MB rotation
LOG_BACKUP_COUNT = 5
```

---

## Project Structure

```
cryptodata/
├── main.py                  # Entrypoint • TUI • Signal scorer • Target engine
├── websocket_client.py      # Binance WS client • Symbol discovery
├── market_data.py           # MarketDataStore • DataFrameManager
├── technical_indicators.py  # Indicators • Depth walls • REST fetchers
├── config.py                # All tunable parameters
├── utils.py                 # Logging • Price/volume formatters
├── requirements.txt         # Python dependencies
├── AGENTS.md                # Context for AI coding assistants
├── .gitignore               # Git ignore rules
└── logs/                    # Log output directory (auto-created)
    └── crypto_scanner.log
```

---

## Development Notes

- **No test suite** — run `python3 main.py` to validate visually
- **No linter/typecheck CI** — `.vscode/settings.json` sets type-checking mode to `"basic"`
- **Logging** is file-only (`logs/crypto_scanner.log`). Rich owns stdout.
- **CoinGecko API** (free tier, ~10–30 calls/min) is called once at startup
- **XAUTUSDT** is always appended after discovery (Binance has `XAUTUSDT`, not `XAUUSDT`)
- **XMRUSDT** is explicitly removed in `main.py`
- Indicators show `N/A` for ~10 seconds after startup until the first kline fetch completes

---

## License

MIT — see [LICENSE](LICENSE) for details.

---

<p align="center">
  <sub>Built with Python, Binance API, and Rich.</sub>
  <br>
  <sub>Data provided by <a href="https://www.binance.com">Binance</a> and <a href="https://www.coingecko.com">CoinGecko</a>.</sub>
</p>
