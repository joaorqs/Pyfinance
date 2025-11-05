from src.core.watchlist import connect, PROJECT_ROOT

DATA_GLOB = (PROJECT_ROOT / "data" / "prices" / "**" / "**" / "*.parquet").as_posix()


def main():
    con = connect()

    # External view over Parquet lake
    con.execute(
        f"""
        CREATE OR REPLACE VIEW v_prices AS
        SELECT * FROM read_parquet('{DATA_GLOB}');
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