BEGIN;

DROP MATERIALIZED VIEW IF EXISTS erc20.view_stablecoins_volume_hourly;

CREATE MATERIALIZED VIEW erc20.view_stablecoins_volume_hourly AS (
with data1 AS (
SELECT date_trunc('hour', evt_block_time) AS hour, contract_address, value FROM erc20."stablecoins_evt_transfer"
),

data2 AS (
SELECT hour, contract_address, SUM(value) OVER (PARTITION BY contract_address, hour ORDER BY hour) AS sum_values
FROM data1
),

data3 AS (
SELECT d.hour, d.contract_address, d.sum_values, s.symbol, s.decimals, s.name FROM data2 d
LEFT JOIN erc20."stablecoins" s
ON d.contract_address = s.contract_address
),

data4 AS (
SELECT *, sum_values/(10^decimals) AS totals FROM data3
)

SELECT hour, totals, symbol, name, contract_address FROM data4
GROUP BY 1,2,3,4,5
ORDER BY hour
);

COMMIT;


INSERT INTO cron.job(schedule, command)
VALUES ('0 0-12/12 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY erc20.view_stablecoins_volume_hourly$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;

















