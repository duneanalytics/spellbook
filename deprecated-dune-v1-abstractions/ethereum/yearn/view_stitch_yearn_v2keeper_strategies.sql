BEGIN;
DROP VIEW IF EXISTS yearn."view_stitch_yearn_v2keeper_strategies" cascade;

CREATE VIEW yearn."view_stitch_yearn_v2keeper_strategies" AS (

  SELECT
    distinct
        add.contract_address as job,
        add._strategy as strategy,
        add."_requiredAmount" as required_amount
  FROM
     yearn_v2."V2KeeperJob_evt_StrategyAdded" add
     LEFT JOIN
        yearn_v2."V2KeeperJob_evt_StrategyRemoved" rm
        ON add._strategy = rm._strategy
        AND add.evt_block_time < rm.evt_block_time
  WHERE
     rm._strategy IS NULL

);
COMMIT;
