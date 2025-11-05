from __future__ import annotations

from datetime import timedelta

import pandas as pd
import streamlit as st

from src.core.watchlist import PROJECT_ROOT, connect, ensure_schema

DATA_ROOT = PROJECT_ROOT / "data" / "prices"
DATA_GLOB = (DATA_ROOT / "**" / "**" / "*.parquet").as_posix()

st.set_page_config(page_title="Market Watchlist", layout="wide")


@st.cache_data(ttl=300)
def list_parquet_files() -> list[str]:
    return [str(p) for p in DATA_ROOT.glob("**/*.parquet")]


@st.cache_data(ttl=300)
def load_dashboard_rows() -> pd.DataFrame:
    parquet_files = list_parquet_files()
    con = connect()
    ensure_schema(con)

    if not parquet_files:
        watchlist_df = con.execute(
            """
            SELECT
                ticker,
                currency,
                zone_low,
                zone_high,
                notify_on_cross,
                cooloff_days,
                tags,
                notes
            FROM watchlist
            ORDER BY ticker;
            """
        ).df()
        con.close()
        watchlist_df["close_now"] = pd.NA
        watchlist_df["close_prev"] = pd.NA
        watchlist_df["pct_change"] = pd.NA
        watchlist_df["last_updated"] = pd.NaT
        watchlist_df["zone_status"] = "No data"
        watchlist_df["crossed_today"] = False
        return watchlist_df

    sql = f"""
        WITH prices AS (
            SELECT * FROM read_parquet('{DATA_GLOB}')
        ), ranked AS (
            SELECT
                Ticker AS ticker,
                Datetime AS datetime,
                Date AS trade_date,
                Value AS close,
                LAG(Value) OVER (PARTITION BY Ticker ORDER BY Datetime DESC) AS prev_close,
                ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Datetime DESC) AS rn
            FROM prices
            WHERE Price = 'Close'
        ), latest AS (
            SELECT * FROM ranked WHERE rn = 1
        )
        SELECT
            w.ticker,
            w.currency,
            w.zone_low,
            w.zone_high,
            w.notify_on_cross,
            w.cooloff_days,
            w.tags,
            w.notes,
            l.datetime AS last_updated,
            l.close AS close_now,
            l.prev_close AS close_prev,
            CASE
                WHEN l.prev_close IS NULL OR l.prev_close = 0 THEN NULL
                ELSE (l.close - l.prev_close) / l.prev_close * 100
            END AS pct_change,
            CASE
                WHEN l.close BETWEEN w.zone_low AND w.zone_high THEN 'In zone'
                WHEN l.close < w.zone_low THEN 'Below'
                WHEN l.close > w.zone_high THEN 'Above'
                ELSE 'No data'
            END AS zone_status,
            CASE
                WHEN l.close BETWEEN w.zone_low AND w.zone_high
                     AND (l.prev_close IS NULL OR NOT (l.prev_close BETWEEN w.zone_low AND w.zone_high))
                THEN TRUE
                ELSE FALSE
            END AS crossed_today
        FROM watchlist w
        LEFT JOIN latest l ON l.ticker = w.ticker
        ORDER BY w.ticker;
    """

    dashboard_df = con.execute(sql).df()
    con.close()
    dashboard_df["last_updated"] = pd.to_datetime(dashboard_df["last_updated"], errors="coerce")
    return dashboard_df


@st.cache_data(ttl=300)
def load_price_history(ticker: str, window_days: int) -> pd.DataFrame:
    parquet_files = list_parquet_files()
    if not parquet_files:
        return pd.DataFrame(columns=["Datetime", "close"])

    sql = f"""
        SELECT
            Datetime AS datetime,
            Value AS close
        FROM read_parquet('{DATA_GLOB}')
        WHERE Ticker = ? AND Price = 'Close'
        ORDER BY Datetime;
    """
    con = connect()
    try:
        df = con.execute(sql, [ticker]).df()
    finally:
        con.close()

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    cutoff = pd.Timestamp.utcnow() - timedelta(days=window_days)
    df = df[df["datetime"] >= cutoff]
    return df


def show_alerts_section(dashboard_df: pd.DataFrame) -> None:
    st.subheader("Alerts")
    alert_rows = dashboard_df[dashboard_df["crossed_today"]]
    if alert_rows.empty:
        st.info("No fresh alerts today. Monitoring...")
        return

    alert_rows = alert_rows[[
        "ticker",
        "close_now",
        "zone_low",
        "zone_high",
        "pct_change",
        "last_updated",
        "notes",
    ]]
    alert_rows = alert_rows.rename(
        columns={
            "ticker": "Ticker",
            "close_now": "Last close",
            "zone_low": "Zone low",
            "zone_high": "Zone high",
            "pct_change": "% vs prev",
            "last_updated": "Updated",
            "notes": "Notes",
        }
    )
    st.dataframe(
        alert_rows,
        hide_index=True,
        use_container_width=True,
        column_config={"% vs prev": st.column_config.NumberColumn(format="%.2f%%")},
    )


@st.cache_data(ttl=300)
def _build_overview_table(dashboard_df: pd.DataFrame) -> pd.DataFrame:
    overview = dashboard_df.copy()
    overview = overview.rename(
        columns={
            "ticker": "Ticker",
            "currency": "CCY",
            "close_now": "Last close",
            "close_prev": "Prev close",
            "pct_change": "% vs prev",
            "zone_low": "Zone low",
            "zone_high": "Zone high",
            "zone_status": "Status",
            "last_updated": "Updated",
            "tags": "Tags",
            "notes": "Notes",
        }
    )
    return overview


def show_overview_section(dashboard_df: pd.DataFrame) -> None:
    st.subheader("Watchlist overview")
    overview = _build_overview_table(dashboard_df)
    st.dataframe(
        overview,
        hide_index=True,
        use_container_width=True,
        column_config={
            "% vs prev": st.column_config.NumberColumn(format="%.2f%%"),
            "Last close": st.column_config.NumberColumn(format="%.2f"),
            "Prev close": st.column_config.NumberColumn(format="%.2f"),
            "Zone low": st.column_config.NumberColumn(format="%.2f"),
            "Zone high": st.column_config.NumberColumn(format="%.2f"),
        },
    )


def show_price_history_section(dashboard_df: pd.DataFrame) -> None:
    st.subheader("Price history")
    tickers = dashboard_df["ticker"].dropna().tolist()
    if not tickers:
        st.info("Price history will appear once market data has been fetched.")
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        selected_ticker = st.selectbox("Ticker", tickers)
        window = st.slider("Window (days)", min_value=7, max_value=180, value=45)

    history_df = load_price_history(selected_ticker, window)
    if history_df.empty:
        st.warning("No price history found for the selected ticker yet.")
        return

    with col2:
        st.metric(
            label=f"{selected_ticker} last close",
            value=f"{history_df['close'].iloc[-1]:.2f}",
        )

    chart_df = history_df.set_index("datetime")
    st.line_chart(chart_df["close"], use_container_width=True)


def main() -> None:
    st.title("Market watchlist dashboard")

    dashboard_df = load_dashboard_rows()
    total = len(dashboard_df)
    in_zone = int((dashboard_df["zone_status"] == "In zone").sum())
    alerts = int(dashboard_df["crossed_today"].sum())

    col1, col2, col3 = st.columns(3)
    col1.metric("Tracked tickers", total)
    col2.metric("In zone", in_zone)
    col3.metric("Alerts today", alerts)

    show_alerts_section(dashboard_df)
    show_overview_section(dashboard_df)
    show_price_history_section(dashboard_df)


if __name__ == "__main__":
    main()
