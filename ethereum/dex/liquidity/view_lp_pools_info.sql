BEGIN;

DROP MATERIALIZED VIEW IF EXISTS dex.view_lp_pools_info;

CREATE MATERIALIZED VIEW dex.view_lp_pools_info AS (
    WITH distinct_pools AS (
        SELECT DISTINCT 
            pool_address,
            project,
            version,
            category
        FROM dex.liquidity
    )
    SELECT
        pool_address,
        (labels.get(pool_address, 'lp_pool_name'))[1] AS pool_name,
        project,
        version,
        category
    FROM distinct_pools
);

CREATE INDEX IF NOT EXISTS dex_view_lp_pools_info_idx ON dex.view_lp_pools_info (pool_address);

-- This script needs to run after the daily insert scripts into `dex.liquidity`: `dex.insert_liquidity_...` 
INSERT INTO cron.job(schedule, command)
VALUES ('7 4 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY dex.view_lp_pools_info$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;
