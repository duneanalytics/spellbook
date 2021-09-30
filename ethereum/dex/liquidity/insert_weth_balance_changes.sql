CREATE OR REPLACE FUNCTION dex.insert_weth_balance_changes(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH all_transfers AS (
    SELECT
        dst AS pool_address,
        date_trunc('day', evt_block_time) AS day,
        wad AS amount
    FROM zeroex."WETH9_evt_Transfer" t
    WHERE evt_block_time >= start_ts and evt_block_time < end_ts
        AND EXISTS (SELECT * FROM dex.liquidity l WHERE t.dst = l.pool_address) 
    UNION ALL
    SELECT
        src,
        date_trunc('day', evt_block_time),
        - wad
    FROM zeroex."WETH9_evt_Transfer" t
    WHERE evt_block_time >= start_ts and evt_block_time < end_ts
        AND EXISTS (SELECT * FROM dex.liquidity l WHERE t.src = l.pool_address) 
    UNION ALL
    SELECT
        dst,
        date_trunc('day', evt_block_time),
        wad
    FROM zeroex."WETH9_evt_Deposit" t
    WHERE evt_block_time >= start_ts and evt_block_time < end_ts
        AND EXISTS (SELECT * FROM dex.liquidity l WHERE t.dst = l.pool_address) 
    UNION ALL
    SELECT
        src,
        date_trunc('day', evt_block_time),
        - wad
    FROM zeroex."WETH9_evt_Withdrawal" t
    WHERE evt_block_time >= start_ts and evt_block_time < end_ts
        AND EXISTS (SELECT * FROM dex.liquidity l WHERE t.src = l.pool_address) 
),
rows AS (
    INSERT INTO dex.daily_balance_changes (
        day,
        pool_address,
        token_address,
        change_amount_raw
    )

    SELECT
        day,
        pool_address,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA AS token_address,
        SUM(amount) AS change_amount_raw
    FROM all_transfers t
    GROUP BY 1, 2, 3

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- First `WETH` event related to LP pools on '2018-05-09'
-- fill 2018 - Q2
SELECT dex.insert_weth_balance_changes(
    '2018-05-09',
    '2018-07-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2018-05-09'
    AND day < '2018-07-01'
);

-- fill 2018 - Q3
SELECT dex.insert_weth_balance_changes(
    '2018-07-01',
    '2018-10-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2018-07-01'
    AND day < '2018-10-01'
);

-- fill 2018 - Q4
SELECT dex.insert_weth_balance_changes(
    '2018-10-01',
    '2019-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2018-10-01'
    AND day < '2019-01-01'
);


-- fill 2019 - Q1
SELECT dex.insert_weth_balance_changes(
    '2019-01-01',
    '2019-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2019-01-01'
    AND day < '2019-04-01'
);

-- fill 2019 - Q2
SELECT dex.insert_weth_balance_changes(
    '2019-04-01',
    '2019-07-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2019-04-01'
    AND day < '2019-07-01'
);

-- fill 2019 - Q3
SELECT dex.insert_weth_balance_changes(
    '2019-07-01',
    '2019-10-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2019-07-01'
    AND day < '2019-10-01'
);

-- fill 2019 - Q4
SELECT dex.insert_weth_balance_changes(
    '2019-10-01',
    '2020-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2019-10-01'
    AND day < '2020-01-01'
);


-- fill 2020 - Q1
SELECT dex.insert_weth_balance_changes(
    '2020-01-01',
    '2020-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2020-01-01'
    AND day < '2020-04-01'
);

-- fill 2020 - Q2
SELECT dex.insert_weth_balance_changes(
    '2020-04-01',
    '2020-07-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2020-04-01'
    AND day < '2020-07-01'
);

-- fill 2020 - Q3
SELECT dex.insert_weth_balance_changes(
    '2020-07-01',
    '2020-10-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2020-07-01'
    AND day < '2020-10-01'
);

-- fill 2020 - Q4
SELECT dex.insert_weth_balance_changes(
    '2020-10-01',
    '2021-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2020-10-01'
    AND day < '2021-01-01'
);

-- fill 2021 - Q1
SELECT dex.insert_weth_balance_changes(
    '2021-01-01',
    '2021-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2021-01-01'
    AND day < '2021-04-01'
);

-- fill 2021 - Q2
SELECT dex.insert_weth_balance_changes(
    '2021-04-01',
    '2021-07-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2021-04-01'
    AND day < '2021-07-01'
);

-- fill 2021 - Q3
SELECT dex.insert_weth_balance_changes(
    '2021-07-01',
    date_trunc('day', now())
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.daily_balance_changes
    WHERE day >= '2021-07-01'
    AND day < date_trunc('day', now())
);


INSERT INTO cron.job (schedule, command)
VALUES ('19 4 * * *', $$
    SELECT dex.insert_weth_balance_changes(
        (SELECT (SELECT max(day) FROM dex.daily_balance_changes) + interval '1 day'),
        (SELECT date_trunc('day', now())));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
