BEGIN;

DROP MATERIALIZED VIEW IF EXISTS erc20."stablecoin_evt_transfer";

CREATE MATERIALIZED VIEW erc20."stablecoin_evt_transfer" AS (
SELECT * FROM erc20."ERC20_evt_Transfer"
WHERE contract_address in (select contract_address from erc20."stablecoins")
);

CREATE INDEX IF NOT EXISTS evt_block_time ON erc20."stablecoins" (evt_block_time, contract_address, evt_block_number, evt_tx_hash, "from", "to", value);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('0 0 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY erc20."stablecoin_evt_transfer"$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
 