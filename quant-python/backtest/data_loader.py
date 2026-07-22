"""
Downloads historical 1-minute crypto bars from Alpaca and caches them to CSV.
Uses the same credentials as the live system (infra/.env).

Cache key is stable per (symbol, months) and refreshes at most once per day
so overlapping date-stamped CSVs do not accumulate.
"""
import os
import sys
import time
import requests
import pandas as pd
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone
from dotenv import load_dotenv

_root = Path(__file__).parent.parent.parent
load_dotenv(_root / "infra" / ".env")

API_KEY = os.getenv("ALPACA_API_KEY", "")
API_SECRET = os.getenv("ALPACA_API_SECRET", "")
BASE_URL = "https://data.alpaca.markets/v1beta3/crypto/us"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CACHE_MAX_AGE_HOURS = 24

DEFAULT_SYMBOLS = [
    "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "DOGE/USD",
    "LINK/USD", "AAVE/USD", "ADA/USD", "BCH/USD", "LTC/USD",
    "AVAX/USD", "UNI/USD", "DOT/USD", "ATOM/USD", "MATIC/USD",
]


def _headers():
    return {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": API_SECRET}


def _cache_path(symbol: str, months: int) -> Path:
    safe = symbol.replace("/", "_")
    return DATA_DIR / f"{safe}_{months}m_1min.csv"


def _cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age_h = (time.time() - path.stat().st_mtime) / 3600
    return age_h < CACHE_MAX_AGE_HOURS


def download_bars(symbol: str, start: str, end: str, timeframe: str = "1Min",
                  cache_file: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch all 1-minute bars for a symbol between start and end (ISO dates).
    Handles pagination. Returns DataFrame columns: t, o, h, l, c, v.
    """
    if cache_file is None:
        cache_file = DATA_DIR / f"{symbol.replace('/', '_')}_{start[:10]}_{end[:10]}.csv"

    if _cache_fresh(cache_file):
        print(f"  [cache] {symbol}")
        df = pd.read_csv(cache_file, parse_dates=["t"])
        return df.sort_values("t").reset_index(drop=True)

    print(f"  [download] {symbol} {start[:10]} → {end[:10]}", end="", flush=True)
    bars = []
    params = {
        "symbols": symbol,
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "limit": 10_000,
        "sort": "asc",
    }

    while True:
        try:
            resp = requests.get(
                f"{BASE_URL}/bars", params=params, headers=_headers(), timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"\n  ERROR fetching {symbol}: {e}")
            break

        chunk = data.get("bars", {}).get(symbol, [])
        bars.extend(chunk)
        print(".", end="", flush=True)

        token = data.get("next_page_token")
        if not token:
            break
        params["page_token"] = token
        time.sleep(0.1)

    print(f" {len(bars)} bars")
    if not bars:
        return pd.DataFrame()

    df = pd.DataFrame(bars)
    df["t"] = pd.to_datetime(df["t"])
    df = df[["t", "o", "h", "l", "c", "v"]].sort_values("t").reset_index(drop=True)
    df["symbol"] = symbol
    df.to_csv(cache_file, index=False)
    return df


def load_all(symbols=None, months=6) -> dict[str, pd.DataFrame]:
    """Download bars for all symbols. Returns {symbol: DataFrame}."""
    if not API_KEY:
        print("ERROR: ALPACA_API_KEY not set in infra/.env")
        sys.exit(1)

    symbols = symbols or DEFAULT_SYMBOLS
    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    from dateutil.relativedelta import relativedelta
    start_dt = datetime.now(timezone.utc) - relativedelta(months=months)
    start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Downloading {len(symbols)} symbols | {months}m window: {start[:10]} → {end[:10]}")
    result = {}
    for sym in symbols:
        df = download_bars(sym, start, end, cache_file=_cache_path(sym, months))
        if not df.empty:
            result[sym] = df
    print(f"\nLoaded {len(result)} symbols, "
          f"{sum(len(d) for d in result.values()):,} total bars\n")
    return result
