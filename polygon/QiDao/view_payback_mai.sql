BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao.view_payback_mai;

CREATE MATERIALIZED VIEW qidao.view_payback_mai AS (
with paybacks as
(
  select * from qidao."erc20QiStablecoin_evt_PayBackToken"
  union all
  select * from qidao."crosschainQiStablecoin_evt_PayBackToken"
  union all
  select * from qidao."CrosschainQiStablecoinV2_evt_PayBackToken"
  union all
  select * from qidao."erc20QiStablecoinwbtc_evt_PayBackToken"
  union all
  select * from qidao."QiStablecoin_evt_PayBackToken"
  order by 1
)
select a."evt_block_time" as "block_time",
       a."evt_tx_hash" as "tx_hash",
       a."contract_address" as "vault_contract_address",
       a."vaultID" as "vault_id",
       b."collateral_contract_address",
       b."collateral_symbol",
       b."collateral_price_symbol",
       a."amount"/(1e18) as "amount",
       a."closingFee"/(10^b."collateral_decimals") as "fee_in_collateral",
       a."amount"/(1e18) * 0.005 as "fee_in_usd"
  from paybacks a left join qidao.view_vault_collateral_mappings b
       on a."contract_address" = b."vault_contract_address"
);

INSERT INTO cron.job(schedule, command)
VALUES ('3 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_payback_mai$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;