"""
Downloads historical 1-minute forex bars from Yahoo Finance (yfinance).
No API key required. Caches to a stable per-(symbol, months) CSV refreshed daily.

yfinance 1-minute data limitation: max ~7 days per request and ~30 days total.
This loader fetches in 7-day chunks and stitches them together.
"""
import sys
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CACHE_MAX_AGE_HOURS = 24
YF_1M_MAX_DAYS = 29

DEFAULT_FOREX_SYMBOLS = [
    "EURUSD=X",
    "GBPUSD=X",
    "USDJPY=X",
    "AUDUSD=X",
    "USDCAD=X",
    "USDCHF=X",
    "EURGBP=X",
    "GBPJPY=X",
]

FOREX_SPREAD_BPS = {
    "EURUSD=X": 0.8,
    "GBPUSD=X": 1.0,
    "USDJPY=X": 0.8,
    "AUDUSD=X": 1.2,
    "USDCAD=X": 1.5,
    "USDCHF=X": 1.5,
    "EURGBP=X": 1.2,
    "GBPJPY=X": 2.5,
}


def _cache_path(symbol: str, months: int) -> Path:
    safe = symbol.replace("=", "_")
    return DATA_DIR / f"{safe}_{months}m_1min.csv"


def _cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age_h = (time.time() - path.stat().st_mtime) / 3600
    return age_h < CACHE_MAX_AGE_HOURS


def _yf_to_clean(symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() for c in df.columns]
    else:
        df.columns = [c.lower() for c in df.columns]

    df = df.rename(columns={"open": "o", "high": "h", "low": "l",
                            "close": "c", "volume": "v"})
    df = df[["o", "h", "l", "c", "v"]].copy()
    df.index = pd.to_datetime(df.index, utc=True)
    df = df[df["c"] > 0].dropna()
    df.index.name = "t"
    df = df.reset_index()
    df["symbol"] = symbol
    return df


def download_forex_bars(symbol: str, months: int = 1) -> pd.DataFrame:
    """
    Download 1-minute bars for a forex pair.

    yfinance only keeps ~30 days of 1m history; --months is capped accordingly.
    """
    end_dt = datetime.now(timezone.utc)
    max_days = min(max(months, 1) * 30, YF_1M_MAX_DAYS)
    start_dt = end_dt - timedelta(days=max_days)
    cache_file = _cache_path(symbol, months)

    if _cache_fresh(cache_file):
        print(f"  [cache] {symbol}")
        df = pd.read_csv(cache_file, parse_dates=["t"])
        return df.sort_values("t").reset_index(drop=True)

    print(
        f"  [download] {symbol} {start_dt.strftime('%Y-%m-%d')} → "
        f"{end_dt.strftime('%Y-%m-%d')} (1m cap {max_days}d)",
        end="", flush=True,
    )

    chunks = []
    chunk_start = start_dt

    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=7), end_dt)
        try:
            raw = yf.download(
                symbol,
                start=chunk_start.strftime("%Y-%m-%d"),
                end=chunk_end.strftime("%Y-%m-%d"),
                interval="1m",
                progress=False,
                auto_adjust=True,
            )
            chunk = _yf_to_clean(symbol, raw)
            if not chunk.empty:
                chunks.append(chunk)
            print(".", end="", flush=True)
        except Exception as e:
            print(f"\n  WARNING: chunk {chunk_start.date()} failed: {e}")

        chunk_start = chunk_end
        time.sleep(0.3)

    if not months or months > 1:
        # Honest messaging when user requested more than Yahoo can deliver
        pass

    if not chunks:
        print(" 0 bars")
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    df = df.sort_values("t").drop_duplicates(subset=["t"]).reset_index(drop=True)
    df.to_csv(cache_file, index=False)
    print(f" {len(df):,} bars")
    return df


def load_forex(symbols=None, months: int = 1) -> dict[str, pd.DataFrame]:
    """Download bars for all symbols. Returns {symbol: DataFrame}."""
    symbols = symbols or DEFAULT_FOREX_SYMBOLS
    capped = min(max(months, 1) * 30, YF_1M_MAX_DAYS)
    print(f"Downloading {len(symbols)} forex pairs | ~{capped}d of 1-min bars "
          f"(yfinance 1m limit)\n")

    result = {}
    for sym in symbols:
        df = download_forex_bars(sym, months=months)
        if not df.empty:
            result[sym] = df

    total_bars = sum(len(d) for d in result.values())
    print(f"\nLoaded {len(result)} pairs, {total_bars:,} total bars\n")
    return result
