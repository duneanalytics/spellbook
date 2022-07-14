BEGIN;
DROP VIEW IF EXISTS yearn."view_stitch_yearn_harvests" cascade;

CREATE VIEW yearn."view_stitch_yearn_harvests" AS(
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

);
COMMIT;
