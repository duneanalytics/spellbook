BEGIN;
DROP VIEW IF EXISTS yearn."view_yearn_harvests" cascade;

CREATE VIEW yearn."view_yearn_harvests" AS(
  SELECT
  hvst.profit_loss / (10^yct."yvault_deposit_token_decimals") as profit_loss,
  hvst.debt_payment / (10^yct."yvault_deposit_token_decimals") as debt_payment,
  hvst.debt_outstanding / (10^yct."yvault_deposit_token_decimals") as debt_outstanding,
  hvst.evt_tx_hash,
  cs."yvault_contract",
  cs."strategy",
  cs."yearn_type",
  yct."yvault_deposit_token",
  yct."yvault_deposit_token_decimals",
  yct."yvault_deposit_token_symbol"

  FROM yearn."view_stitch_yearn_harvests" hvst
  LEFT JOIN yearn."view_yearn_contract_strategy" cs ON hvst."strategy" = cs."strategy"
  LEFT JOIN yearn."view_yearn_contract_tokens" yct ON cs."yvault_contract" = yct."yvault_contract"
  WHERE
  "yvault_deposit_token_symbol" is not null
);
COMMIT;
