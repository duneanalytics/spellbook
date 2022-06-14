CREATE MATERIALIZED VIEW tokemak.view_tokemak_lookup_sources
(
	id, source_name
) AS (
    SELECT 0 as id, 'Undefined' as source_name
    UNION
    SELECT 1 as id, 'Curve' as source_name
    UNION
    SELECT 2 as id, 'Convex' as source_name
    UNION
    SELECT 3 as id, 'Sushiswap' as source_name
    UNION
    SELECT 4 as id, 'UniswapV2' as source_name
);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_lookup_sources (
   id
);

INSERT INTO cron.job(schedule, command)
VALUES ('1 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_lookup_sources$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;