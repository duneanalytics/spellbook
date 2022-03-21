
BEGIN;
DROP VIEW IF EXISTS yearn."view_yearn_harvests" cascade;

CREATE VIEW yearn."view_yearn_harvests" AS(
  SELECT
    CASE
      WHEN (hvst."profit"/(10^yct."yvault_deposit_token_decimals")) > 0 THEN (hvst."profit"/(10^yct."yvault_deposit_token_decimals"))
      ELSE (hvst."loss"/(10^yct."yvault_deposit_token_decimals"))*-1 END as profit_loss,
    (hvst."debtPayment"/(10^yct."yvault_deposit_token_decimals")) as debtPayment,
    (hvst."debtOutstanding"/(10^yct."yvault_deposit_token_decimals")) as debtOutstanding,
    hvst."evt_tx_hash",
    cs."yvault_contract",
    cs."strategy",
    cs."yearn_type",
    yct."yvault_deposit_token",
    yct."yvault_deposit_token_decimals",
    yct."yvault_deposit_token_symbol"
  FROM
    yearn."yvault_strat_evt_Harvested" hvst left join yearn."view_yearn_contract_strategy" cs on hvst."contract_address" = cs."strategy"
    left join yearn."view_yearn_contract_tokens" yct on cs."yvault_contract" = yct."yvault_contract"
  WHERE
    "yvault_deposit_token_symbol" is not null
)
