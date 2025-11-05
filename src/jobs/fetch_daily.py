from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pandas as pd
import yfinance as yf
import pyarrow.fs as pafs
from pandas import DataFrame

from src.core.watchlist import sync_watchlist_replace, get_tickers, PROJECT_ROOT

DATA_ROOT = PROJECT_ROOT / "data" / "prices"



def write_partitioned_parquet(df: pd.DataFrame, base_dir) -> None:
    if df.empty:
        return

    # Wide (Price x Ticker) -> long tidy
    df_long = (
        df.stack('Ticker', future_stack=True)
        .stack(0, future_stack=True)
        .rename_axis(['Datetime', 'Ticker', 'Price'])
        .reset_index(name='Value')
    )

    # Derive partition column
    df_long['Date'] = pd.to_datetime(df_long['Datetime']).dt.date.astype(str)

    # Arrow table (no index)
    table = pa.Table.from_pandas(df_long, preserve_index=False)

    ds.write_dataset(
        table,
        base_dir=str(base_dir),
        format="parquet",
        partitioning=ds.partitioning(
            schema=pa.schema([
                pa.field('Ticker', pa.string()),
                pa.field('Date', pa.string()),
            ]),
            flavor='hive'
        ),
    existing_data_behavior="overwrite_or_ignore",
    )


def fetch_ticker(tickers: list, days: int = 365) -> DataFrame | None:
    df = yf.download(tickers, period=f"{days}d", interval="1h", auto_adjust=False)
    if df is None or df.empty:
        print(f"[WARN] No data for {tickers}")
        return None

    return df


def main(data_dir: Path = DATA_ROOT):
    # 1) Sync watchlist (adds/removes to match YAML exactly)
    sync_watchlist_replace()

    # 2) Fetch & store
    tickers = get_tickers()
    if not tickers:
        print("[WARN] Watchlist is empty. Nothing to fetch.")
        return
    df = fetch_ticker(tickers)
    write_partitioned_parquet(df, DATA_ROOT)
    print(f"[OK] {tickers}: {len(df)} rows")


if __name__ == "__main__":
    main(DATA_ROOT)