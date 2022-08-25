BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol.view_daily_average_prices;
CREATE MATERIALIZED VIEW gnosis_protocol.view_daily_average_prices AS

WITH
days as (
  SELECT date(time_series) as day
    FROM generate_series('2020-01-01'::timestamp, now(), '1 day') as time_series
),

gp_prices as (
    SELECT symbol, date(price_date) as day, AVG(token_usd_price) as average_price
    FROM gnosis_protocol."view_price_batch"
    GROUP BY symbol, day
),

daily_prices as (
    SELECT
        day,
        symbol,
        average_price
    FROM (
        SELECT 
            d.day,
            p.symbol,
            p.average_price,
            ROW_NUMBER () OVER (
        		PARTITION BY d.day, p.symbol
        		ORDER BY p.day desc
        	) as row
        FROM days d, gp_prices p
        WHERE 
            p.day <= d.day
    ) a WHERE row = 1
)

SELECT 
    *,
    -100 * (LAG(average_price) OVER (
        PARTITION BY symbol
        ORDER BY day
    ) - average_price) / average_price AS price_change
FROM daily_prices;

CREATE UNIQUE INDEX IF NOT EXISTS view_daily_average_prices_id ON gnosis_protocol.view_daily_average_prices (day, symbol) ;
CREATE INDEX view_daily_average_prices_1 ON gnosis_protocol.view_daily_average_prices (day);
CREATE INDEX view_daily_average_prices_2 ON gnosis_protocol.view_daily_average_prices (symbol);

-- INSERT INTO cron.job (schedule, command)
-- -- Every 6 hours.
-- VALUES ('0 */6 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol.view_daily_average_prices')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
-- COMMIT;
