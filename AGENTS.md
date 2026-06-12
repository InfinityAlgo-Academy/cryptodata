# CryptoData — AGENTS.md

## Run

```sh
python3 main.py
```

Requires a real terminal (Rich `Live` display). No tests, no linter, no typecheck CI.

## Architecture

- **`main.py`** — asyncio entrypoint. Spawns 5 background tasks: WS client, RSI klines, display loop, DataFrame snapshot, technical indicators.
- **Symbol discovery** (`websocket_client.py:fetch_top_symbols`): CoinGecko market cap → Binance pair cross-ref. Falls back to Binance volume sort, then hardcoded list.
- **Data flow**: Binance WebSocket (combined stream, `<symbol>@ticker`) → `MarketDataStore` → Rich TUI table + `DataFrameManager` (rolling CSV export).
- **Config**: all tunable params in `config.py` (symbol count, stablecoins, display intervals, WS reconnect settings).

## Key conventions & quirks

- **Logging**: file-only (`logs/crypto_scanner.log`). No console output — Rich owns stdout.
- **Stablecoin filter** (`config.py:STABLECOINS`): symbols containing these substrings are excluded. Keep this list current.
- **XAUTUSDT**: always appended to the symbol list after filtering (Binance has `XAUTUSDT`, not `XAUUSDT`).
- **XMRUSDT**: explicitly removed in `main.py:561`.
- **Indicators**: show "N/A" for ~10s after startup until first kline batch completes.
- **CoinGecko API**: free tier ~10–30 calls/min, called once at startup — safe.
- **No `README` or `__init__.py`** files.
- **`.vscode/settings.json`** sets `python.analysis.typeCheckingMode` to `"basic"` (not strict).
