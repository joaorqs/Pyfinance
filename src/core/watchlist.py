import yaml
import duckdb
from dataclasses import dataclass
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "db" / "markets.duckdb"
WATCHLIST_YAML = PROJECT_ROOT / "data" / "watchlist.yaml"

@dataclass
class WatchItem:
    ticker: str
    currency: str = "USD"
    zone_low: float = 0.0
    zone_high: float = 0.0
    notify_on_cross: bool = True
    cooloff_days: int = 1
    tags: str = ""
    notes: str = ""


def connect() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS watchlist (
            ticker TEXT PRIMARY KEY,
            currency TEXT,
            zone_low DOUBLE,
            zone_high DOUBLE,
            notify_on_cross BOOLEAN,
            cooloff_days INTEGER,
            tags TEXT,
            notes TEXT,
            updated_at TIMESTAMP DEFAULT now()
        );
        """
    )


def load_watchlist_yaml(path: Path = WATCHLIST_YAML) -> List[WatchItem]:
    if not path.exists():
        raise FileNotFoundError(f"Watchlist YAML not found at {path}")
    data = yaml.safe_load(path.read_text()) or {}
    items = []
    for r in data.get("watchlist", []):
        tags = r.get("tags", [])
        tags_str = ",".join(tags) if isinstance(tags, list) else (tags or "")
        items.append(
            WatchItem(
                ticker=r["ticker"],
                currency=r.get("currency", "USD"),
                zone_low=float(r["zone_low"]),
                zone_high=float(r["zone_high"]),
                notify_on_cross=bool(r.get("notify_on_cross", True)),
                cooloff_days=int(r.get("cooloff_days", 1)),
                tags=tags_str,
                notes=r.get("notes", ""),
            )
        )
    return items


def sync_watchlist_replace() -> None:
    """
    Replace DuckDB `watchlist` with YAML exactly:
    - Adds new tickers
    - Updates existing
    - Removes rows not present in YAML
    """
    con = connect()
    ensure_schema(con)
    items = load_watchlist_yaml()

    # Create a temp table and swap-in to guarantee exact match
    con.execute(
        """
        CREATE TEMP TABLE tmp_watchlist AS SELECT * FROM watchlist WHERE 1=0;
        """
    )

    for it in items:
        con.execute(
            """
            INSERT INTO tmp_watchlist (ticker,currency,zone_low,zone_high,notify_on_cross,cooloff_days,tags,notes,updated_at)
            VALUES (?,?,?,?,?,?,?,?, now());
            """,
            [
                it.ticker,
                it.currency,
                it.zone_low,
                it.zone_high,
                it.notify_on_cross,
                it.cooloff_days,
                it.tags,
                it.notes,
            ],
        )

# Deduplicate inside DuckDB before swap-in
    con.execute("BEGIN TRANSACTION;")
    con.execute("DELETE FROM watchlist;")
    con.execute(
        """
        INSERT INTO watchlist
        SELECT
            ticker,
            ANY_VALUE(currency),
            ANY_VALUE(zone_low),
            ANY_VALUE(zone_high),
            BOOL_OR(notify_on_cross),
            MAX(cooloff_days),
            ANY_VALUE(tags),
            ANY_VALUE(notes),
            MAX(updated_at)
        FROM tmp_watchlist
        GROUP BY ticker;
        """
    )
    con.execute("COMMIT;")
    con.close()

def get_tickers() -> List[str]:
    con = connect()
    ensure_schema(con)
    rows = con.execute("SELECT ticker FROM watchlist ORDER BY ticker").fetchall()
    con.close()
    return [r[0] for r in rows]