CREATE OR REPLACE FUNCTION uniswap_v3.insert_uniswap_v3_poolcreated(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO uniswap_v3.view_pools(
        token0,
        token1,
        fee,
        pool
    )

    SELECT "token0","token1", fee, pool FROM uniswap_v3."Factory_evt_PoolCreated" pc
    WHERE NOT EXISTS (SELECT 1 FROM uniswap_v3.view_pools vp WHERE vp.pool = pc.pool LIMIT 1)

    ON CONFLICT DO NOTHING
    RETURNING 1
   )
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2021 (post-regenesis 11-11)
SELECT uniswap_v3.insert_uniswap_v3_poolcreated(
    '2021-11-10',
    now(),
    0,
    (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes')
);

INSERT INTO cron.job (schedule, command)
VALUES ('27,57 * * * *', $$
    SELECT uniswap_v3.insert_uniswap_v3_poolcreated(
        '2021-11-11'::timestamptz,
        (SELECT now() - interval '20 minutes'),
        0,
        (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes')
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
