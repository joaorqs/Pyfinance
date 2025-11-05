from src.core.watchlist import connect

# Minimal alert candidate query (print only)
SQL = """
WITH last2 AS (
  SELECT ticker, date, close,
         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) rn
  FROM v_prices
), now_yday AS (
  SELECT a.ticker, a.close AS close_now, b.close AS close_prev,
         w.zone_low, w.zone_high
  FROM last2 a
  JOIN last2 b ON a.ticker=b.ticker AND b.rn=2
  JOIN watchlist w ON w.ticker=a.ticker
  WHERE a.rn=1 AND w.notify_on_cross
)
SELECT * FROM now_yday
WHERE close_now BETWEEN zone_low AND zone_high
  AND NOT (close_prev BETWEEN zone_low AND zone_high);
"""


def main():
    con = connect()
    try:
        rows = con.execute(SQL).fetchall()
        for r in rows:
            print("ALERT:", r)
    finally:
        con.close()


if __name__ == "__main__":
    main()