BEGIN;
DROP MATERIALIZED VIEW IF EXISTS yearn."view_yearn_strategy_agg_data" cascade;

CREATE MATERIALIZED VIEW yearn."view_yearn_strategy_agg_data" AS (
  SELECT
    strategy,
    COUNT(evt_tx_hash) as harvest_count,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY (gas_used/n_harvests_per_tx)::integer) AS harvest_median_gas,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY harvested_ago) AS harvest_median_time
  FROM
    (
      SELECT
        strategy,
        count(strategy) over (PARTITION BY evt_tx_hash) as n_harvests_per_tx,
        evt_tx_hash,
        evt_block_time - lag(evt_block_time) over (
          PARTITION BY strategy
          ORDER BY
            evt_block_time
        ) harvested_ago,
        gas_used
      FROM
        yearn."view_stitch_yearn_harvests" hvst
        INNER JOIN ethereum.transactions tx ON hvst.evt_tx_hash = tx.hash
    ) df
  GROUP BY
    strategy
);

INSERT INTO cron.job (schedule, command)
VALUES ('0 0 * * *', 'REFRESH MATERIALIZED VIEW yearn."view_yearn_strategy_agg_data";')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;
