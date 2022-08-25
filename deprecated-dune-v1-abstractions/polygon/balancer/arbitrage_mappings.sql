BEGIN;

CREATE OR REPLACE FUNCTION balancer.arbitrage_mappings(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT into balancer.address_mappings (
        address,
        label,
        type,
        author
    )
    SELECT
        DISTINCT(t.to) AS address,
        'arbitrage bot' AS label,
        'dapp usage' AS type,
        'balancerlabs' AS author
    FROM dex.trades t1
    INNER JOIN dex.trades t2
    ON t1.tx_hash = t2.tx_hash
    AND t1.token_a_address = t2.token_b_address
    AND t1.token_b_address = t2.token_a_address
    AND ((t1.project = 'Balancer' AND t2.project = 'Quickswap') or (t1.project = 'Quickswap' AND t2.project = 'Balancer'))
    INNER JOIN polygon.transactions t ON t.hash = t1.tx_hash
    WHERE t1.block_time >= start_ts and t1.block_time < end_ts
    AND t2.block_time >= start_ts and t2.block_time < end_ts
    AND t.to NOT IN (
        select address
        from balancer.address_mappings
        where author = 'balancerlabs'
        and type = 'balancer_source'
    )
    UNION ALL
    SELECT
        DISTINCT(t.to) AS address,
        'arbitrage bot' AS label,
        'dapp usage' AS type,
        'balancerlabs' AS author
    FROM dex.trades t1
    INNER JOIN dex.trades t2
    ON t1.tx_hash = t2.tx_hash
    AND t1.token_a_address = t2.token_b_address
    AND t1.token_b_address = t2.token_a_address
    AND ((t1.project = 'Balancer' AND t2.project = 'Sushiswap') or (t1.project = 'Sushiswap' AND t2.project = 'Balancer'))
    INNER JOIN polygon.transactions t ON t.hash = t1.tx_hash
    WHERE t1.block_time >= start_ts and t1.block_time < end_ts
    AND t2.block_time >= start_ts and t2.block_time < end_ts
    AND t.to NOT IN (
        select address
        from balancer.address_mappings
        where author = 'balancerlabs'
        and type = 'balancer_source'
    )
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2021
SELECT balancer.arbitrage_mappings('2021-01-01', '2021-02-01');
SELECT balancer.arbitrage_mappings('2021-02-01', '2021-03-01');
SELECT balancer.arbitrage_mappings('2021-03-01', '2021-04-01');
SELECT balancer.arbitrage_mappings('2021-04-01', '2021-05-01');
SELECT balancer.arbitrage_mappings('2021-05-01', '2021-06-01');
SELECT balancer.arbitrage_mappings('2021-06-01', '2021-07-01');
SELECT balancer.arbitrage_mappings('2021-07-01', '2021-08-01');
SELECT balancer.arbitrage_mappings('2021-08-01', '2021-09-01');
SELECT balancer.arbitrage_mappings('2021-09-01', '2021-10-01');
SELECT balancer.arbitrage_mappings('2021-10-01', '2021-11-01');
SELECT balancer.arbitrage_mappings('2021-11-01', now());

-- daily update
INSERT INTO cron.job (schedule, command)
VALUES ('1 22 * * *', $$SELECT balancer.arbitrage_mappings((SELECT now() - interval '3 days'), now());$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;
