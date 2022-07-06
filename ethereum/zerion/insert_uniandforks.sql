CREATE OR REPLACE FUNCTION zerion.insert_uniandforks(start_ts timestamptz, end_ts timestamptz=NOW(), start_block numeric=0, end_block numeric=9e18) RETURNS INTEGER
LANGUAGE plpgsql AS $function$
DECLARE r INTEGER;
BEGIN

WITH uni_and_forks AS (
    SELECT tx_hash 
    FROM ethereum.traces traces
    WHERE traces.to IN ( 
        '\xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F', -- Sushi
        '\x7a250d5630B4cF539739dF2C5dAcb4c659F2488D', -- Uni 
        '\xE592427A0AEce92De3Edee1F18E0157C05861564' -- Uni v3
        )
    AND position('\x7a6572696f6e' in input::bytea) > 260
    AND block_time >= start_ts AND block_time < end_ts
)

, rows AS (
    INSERT INTO zerion.trades (
	block_time
    , trader
    , usd_volume
    , protocol
    , tx_hash
    , sold_token_amount
    , bought_token_amount
    , sold_token_address
    , bought_token_address
    , sold_token_symbol
    , bought_token_symbol
    )
    SELECT block_time
    , trader_a AS trader
    , usd_amount AS usd_volume
    , project AS protocol
    , tx_hash
    , token_b_amount AS sold_token_amount
    , token_a_amount AS bought_token_amount
    , token_b_address AS sold_token_address
    , token_a_address AS bought_token_address
    , token_b_symbol AS sold_token_symbol
    , token_a_symbol AS bought_token_symbol
    FROM dex.trades
    WHERE block_time >= start_ts
    AND block_time < end_ts
    AND tx_hash IN (SELECT tx_hash FROM uni_and_forks)
    AND tx_hash NOT IN (SELECT tx_hash FROM zerion.trades WHERE block_time >= start_ts AND block_time < end_ts)

    ON CONFLICT DO NOTHING
    RETURNING 1
    )
    SELECT count(*) INTO r from rows;
    RETURN r;
    END
    $function$;

-- fill 2020
SELECT zerion.insert_uniandforks(
    '2020-01-01',
    '2021-01-01',
    (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < '2021-01-01')
)
;

fill 2021
SELECT zerion.insert_uniandforks(
    '2021-01-01',
    '2022-01-01',
    (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < '2022-01-01')
)
;

-- fill 2022
SELECT zerion.insert_uniandforks(
    '2022-01-01',
    NOW(),
    (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2022-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < NOW() - interval '20 minutes')
)
;


INSERT INTO cron.job (schedule, command)
VALUES ('*/15 * * * *', $$
    SELECT zerion.insert_uniandforks(
        (SELECT MAX(block_time) - interval '6 hours' FROM zerion.trades),
        (SELECT now() - interval '20 minutes'),
        (SELECT MAX(number) FROM ethereum.blocks WHERE time < (SELECT MAX(block_time) - interval '6 hours' FROM zerion.trades)),
        (SELECT MAX(number) FROM ethereum.blocks where time < NOW() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
