-- Reporting view: close price with a 20-day moving average.
-- ma20 is NULL until a ticker has 20 rows of history.
CREATE OR REPLACE VIEW workspace.stocks.v_price_trend AS
SELECT ticker, trade_date, close,
       CASE WHEN COUNT(*) OVER w = 20
            THEN AVG(close) OVER w
       END AS ma20
FROM workspace.stocks.fact_daily_price
WINDOW w AS (PARTITION BY ticker ORDER BY trade_date
             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
