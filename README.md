# CryptoData

Real-time Binance crypto market scanner with a Rich-powered terminal UI. Tracks top 20 non-stablecoin USDT pairs by market cap, computes technical indicators, order-book walls, and composite action signals — all live in a flicker-free terminal.

## Features

- **Live TUI** — flicker-free Rich table with 20+ columns: price, change, volume, spreads, order-book walls, VWAP, MAs, Bollinger Bands, Fibonacci, RSI (5-period), composite action signal, and pivot-based targets.
- **Composite Action Signal** — STRONG BUY / BUY / LEAN BUY / HOLD / LEAN SELL / SELL / STRONG SELL scored from RSI5, BB%, Fib%, MA20/MA50/MA200 distance, VWAP distance, and relative volume.
- **Smart Targets** — ranks R1/R2/BB/Fib 127%–161.8%/VWAP/order-book walls/round numbers by RR ratio × confluence. Shows best target with label, price, confluence bolts, and RR ratio.
- **Order-Book Walls** — detects largest bid/ask clusters from depth snapshots (100 levels, grouped at 0.2%). Shows cluster price + volume in millions.
- **Fibonacci Golden Zone** — highlights 50%–61.8% retracement levels in green.
- **Volume Surge Detection** — WHALES column shows dominant order-book wall volume; R VOL column shows relative volume vs 24h average.
- **Symbol Discovery** — CoinGecko market-cap ranking with Binance cross-reference and volume fallback.
- **Logging** — file-only (`logs/crypto_scanner.log`), no console interference.

## Quick Start

```bash
git clone https://github.com/InfinityAlgo-Academy/cryptodata.git
cd cryptodata
pip install -r requirements.txt
python3 main.py
```

Requires a real terminal — the Rich `Live` display will not render in non-interactive shells.

## Configuration

All tunable parameters live in `config.py`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `TOP_N_SYMBOLS` | 20 | Number of symbols to track |
| `STABLECOINS` | — | List of stablecoin substrings to exclude |
| `DISPLAY_REFRESH_INTERVAL` | 1.0s | TUI refresh rate |
| `PRICE_DECIMALS` | 4 | Decimal places for prices |
| `RECONNECT_BASE_DELAY` | 1.0s | Initial WS reconnect delay |

## Columns

| Column | Description |
|--------|-------------|
| № | Rank |
| SYMBOL | Trading pair (full name, e.g. BTCUSDT) |
| ACT | Composite action signal |
| TGT | Best target with label, confluence, RR ratio |
| TICK | Tick direction (↑ ↓ →) |
| PRICE | Last price |
| CHG% | 24h change |
| SPR | Bid-ask spread (bps) |
| OB | Best bid/ask wall cluster (price + volume) |
| VOL | 24h quote volume |
| R VOL | Relative volume vs 24h average |
| WHALES | Dominant wall side + volume |
| VWAP | % distance from VWAP |
| MA20 | % distance from 20-period SMA |
| MA50 | % distance from 50-period SMA |
| MA200 | % distance from 200-period SMA |
| BB% | Bollinger Band % position |
| ATR | Average True Range (% of price) |
| FIB | Fibonacci retracement level (50–61.8% = green) |
| RSI | 5-period RSI |
| HIGH | 24h high |
| LOW | 24h low |
| TIME | Last update timestamp |

## Architecture

```
main.py                 → Entrypoint, TUI layout, signal scorer
websocket_client.py     → Binance combined-stream WS client + symbol discovery
market_data.py          → Thread-safe data store + DataFrame manager
technical_indicators.py → SMA, BB, VWAP, pivot R/S, Fib, ATR, depth walls
config.py               → All tunable parameters
utils.py                → Logging, price/volume formatting
```

## Requirements

- Python 3.9+
- `websockets>=13.0`
- `pandas>=2.0.0`
- `rich>=13.7.1`
