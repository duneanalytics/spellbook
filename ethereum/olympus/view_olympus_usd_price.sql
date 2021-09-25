CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.olympus_usd_price;
create materialized view olympus.olympus_usd_price as 

with time as
(
SELECT
generate_series('2021-03-31 00:00', NOW(), '1 day'::interval) as Date
),

swap AS ( 
        SELECT
            date_trunc('day', sw."evt_block_time") AS day,
            ("amount0In" + "amount0Out")/1e9 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
            
        FROM sushi."Pair_evt_Swap" sw
        WHERE contract_address = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-03-22'
        ), 
		
		a1_prcs AS (
        SELECT avg(price) a1_prc, date_trunc('day', minute) AS day
        FROM prices.usd
        WHERE minute >= '2021-03-22'
        and contract_address ='\x6b175474e89094c44da98b954eedeac495271d0f'
        group by 2
        ),

price as ( SELECT
    a1_prcs."day" AS "date", 
    (AVG((a1_amt/a0_amt)*a1_prc)) AS price
FROM swap 
JOIN a1_prcs ON swap."day" = a1_prcs."day"
GROUP BY 1
ORDER BY 1 desc)


select * from price

; 

CREATE INDEX IF NOT EXISTS "date" ON olympus.olympus_usd_price ("date", price);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 59 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.olympus_usd_price$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;