BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao.view_deposit_collateral;

CREATE MATERIALIZED VIEW qidao.view_deposit_collateral AS (
with deposits as
(
  select * from qidao."erc20QiStablecoin_evt_DepositCollateral"
  union all
  select * from qidao."crosschainQiStablecoin_evt_DepositCollateral"
  union all
  select * from qidao."CrosschainQiStablecoinV2_evt_DepositCollateral"
  union all
  select * from qidao."erc20QiStablecoinwbtc_evt_DepositCollateral"
  union all
  select * from qidao."QiStablecoin_evt_DepositCollateral"
  order by 1
)
select a."evt_block_time" as "block_time",
       a."evt_tx_hash" as "tx_hash",
       a."contract_address" as "vault_contract_address",
       a."vaultID" as "vault_id",
       b."collateral_contract_address",
       b."collateral_symbol",
       b."collateral_price_symbol",
       a."amount"/(10^b."collateral_decimals") as "amount"
  from deposits a left join qidao.view_vault_collateral_mappings b
       on a."contract_address" = b."vault_contract_address"
);

INSERT INTO cron.job(schedule, command)
VALUES ('3 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_deposit_collateral$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;