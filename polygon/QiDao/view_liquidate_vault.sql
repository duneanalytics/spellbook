BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao.view_liquidate_vault;

CREATE MATERIALIZED VIEW qidao.view_liquidate_vault AS (
with liquidates as
(
  select * from qidao."erc20QiStablecoin_evt_LiquidateVault"
  union all
  select * from qidao."crosschainQiStablecoin_evt_LiquidateVault"
  union all
  select * from qidao."CrosschainQiStablecoinV2_evt_LiquidateVault"
  union all
  select * from qidao."erc20QiStablecoinwbtc_evt_LiquidateVault"
  order by 1
)
select a."evt_block_time" as "block_time",
       a."evt_tx_hash" as "tx_hash",
       a."contract_address" as "vault_contract_address",
       a."vaultID" as "vault_id",
       a."owner" as "owner_address",
       a."buyer" as "buyer_address",
       b."collateral_contract_address",
       b."collateral_symbol",
       b."collateral_price_symbol",
       a."debtRepaid"/(1e18) as "mai_repaid",
       a."collateralLiquidated"/(1e18) as "collateral_liquidated",
       a."closingFee"/(10^b."collateral_decimals") as "fee_in_collateral",
       a."debtRepaid"/(1e18) * 0.005 as "fee_in_usd"
  from liquidates a left join qidao.view_vault_collateral_mappings b
       on a."contract_address" = b."vault_contract_address"
);

INSERT INTO cron.job(schedule, command)
VALUES ('3 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_liquidate_vault$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;