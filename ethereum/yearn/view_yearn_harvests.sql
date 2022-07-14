
BEGIN;
DROP VIEW IF EXISTS yearn."view_yearn_harvests" cascade;

CREATE VIEW yearn."view_yearn_harvests" AS(
  SELECT
  hvst.profit_loss / (10^yct."yvault_deposit_token_decimals") as profit_loss,
  hvst.debt_payment / (10^yct."yvault_deposit_token_decimals") as debt_payment,
  hvst.debt_outstanding / (10^yct."yvault_deposit_token_decimals") as debt_outstanding,
  hvst.evt_block_time,
  hvst.evt_tx_hash,
  cs."yvault_contract",
  cs."strategy",
  cs."yearn_type",
  yct."yvault_deposit_token",
  yct."yvault_deposit_token_decimals",
  yct."yvault_deposit_token_symbol"

  FROM
  (
    SELECT
      v2."contract_address",
      CASE
        WHEN v2."profit" > 0 THEN (v2."profit")
        ELSE (v2."loss")*-1 END as profit_loss,
      v2."debtPayment" as debt_payment,
      v2."debtOutstanding" as debt_outstanding,
      v2."evt_block_time",
      v2."evt_tx_hash"
    FROM
      yearn."yvault_strat_evt_Harvested" v2
  UNION ALL

    SELECT
      v1."contract_address",
      v1."wantEarned" as profit_loss,
      NULL as debt_payment,
      NULL as debt_outstanding,
      v1."evt_block_time",
      v1."evt_tx_hash"
    FROM
    yearn."yearn_v1_strat_evt_Harvested" v1

  ) hvst

  LEFT JOIN yearn."view_yearn_contract_strategy" cs ON hvst."contract_address" = cs."strategy"
  LEFT JOIN yearn."view_yearn_contract_tokens" yct ON cs."yvault_contract" = yct."yvault_contract"
  WHERE
  "yvault_deposit_token_symbol" is not null
);
COMMIT;
