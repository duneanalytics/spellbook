BEGIN;
DROP VIEW IF EXISTS yearn."view_stitch_yearn_v2keeper_jobs" cascade;

CREATE VIEW yearn."view_stitch_yearn_v2keeper_jobs" AS (

  SELECT
     distinct add._job as job
  FROM
     yearn_v2."V2Keeper_evt_JobAdded" add
     LEFT JOIN
        yearn_v2."V2Keeper_evt_JobRemoved" rm
        ON add._job = rm._job
        AND add.evt_block_time < rm.evt_block_time
  WHERE
     rm._job IS NULL

);
COMMIT;
