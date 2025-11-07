from pathlib import Path

from src.core.watchlist import PROJECT_ROOT, connect

DATA_ROOT = PROJECT_ROOT / "data" / "prices"
# ``read_parquet`` only supports a single ``**`` segment in glob expressions, so we
# build the pattern manually instead of chaining two of them. This still allows the
# reader to recurse through any nested folders under ``data/prices`` while avoiding
# the ``duckdb.duckdb.IOException: IO Error: Cannot use multiple '**' in one path``
# failure that was raised previously.
DATA_GLOB = (DATA_ROOT / "**" / "*.parquet").as_posix()


def _parquet_files(root: Path = DATA_ROOT) -> list[Path]:
    if not root.exists():
        return []
    return [p for p in root.rglob("*.parquet") if p.is_file()]


def main():
    con = connect()
    parquet_files = _parquet_files()

    # External view over Parquet lake
    if parquet_files:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW v_prices AS
            SELECT * FROM read_parquet('{DATA_GLOB}');
            """
        )
    else:
        con.execute(
            """
            CREATE OR REPLACE VIEW v_prices AS
            SELECT
                CAST(NULL AS TEXT) AS ticker,
                CAST(NULL AS DATE) AS date,
                CAST(NULL AS DOUBLE) AS close
            WHERE 1=0;
            """
        )

    # Optional: materialize for faster dashboards (idempotent refresh)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prices AS SELECT * FROM v_prices WHERE 1=0;
        DELETE FROM prices;
        INSERT INTO prices SELECT * FROM v_prices;
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_prices ON prices(ticker, date);")
    con.close()
    print("[OK] Views/materialized tables built.")


if __name__ == "__main__":
    main()