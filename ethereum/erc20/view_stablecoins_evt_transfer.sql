BEGIN;

DROP MATERIALIZED VIEW IF EXISTS erc20."stablecoin_evt_transfer";

CREATE MATERIALIZED VIEW erc20."stablecoin_evt_transfer" AS (
SELECT * FROM erc20."ERC20_evt_Transfer"
WHERE contract_address in (select contract_address from erc20."stablecoins")
LIMIT 100
);

SELECT * from erc20."stablecoin_evt_transfer"

INSERT INTO cron.job(schedule, command)
VALUES ('*/12 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY erc20."stablecoin_evt_transfer"$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
