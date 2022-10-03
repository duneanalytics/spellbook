{{ config(
    schema = 'bancor_v2_ethereum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id', 'tx_hash', 'evt_index', 'version'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "bancor_v2",
                                \'["codingsh"]\') }}'
    )
}}
WITH dexs AS
(
     -- Bancor v2
        SELECT
            d.day,
            'Bancor' AS project,
            '2' AS version,
            'DEX' AS category,
            balances.amount_raw AS token_amount_raw,
            balances.token_address,
            balances.wallet_address AS pool_address,
            balances.token_index,
            CAST(NULL as double) AS token_pool_percentage
        FROM balances
        INNER JOIN days d ON balances.day <= d.day AND d.day < balances.next_day
        LEFT JOIN erc20.tokens erc20 on erc20.contract_address = dexs.token_address
        LEFT JOIN prices.usd p on p.contract_address = dexs.token_address and p.minute = dexs.day
            AND p.minute >= start_ts
            AND p.minute < end_ts
        {% if is_incremental() %}
        AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)
-- First bancor."StandardPoolConverter_evt_LiquidityAdded" evt on '2020-12-14'
-- fill 2020 - Q4
SELECT dex.insert_liquidity_bancor_v2(
    '2020-10-01',
    '2021-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2020-10-01'
    AND day < '2021-01-01'
    AND project = 'Bancor'
    AND version = '2'
);

-- fill 2021 - Q1
SELECT dex.insert_liquidity_bancor_v2(
    '2021-01-01',
    '2021-04-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2021-01-01'
    AND day < '2021-04-01'
    AND project = 'Bancor'
    AND version = '2'
);

-- fill 2021 Q2 + Q3
SELECT dex.insert_liquidity_bancor_v2(
    '2021-04-01',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.liquidity
    WHERE day >= '2021-04-01'
    AND day < now() - interval '20 minutes'
    AND project = 'Bancor'
    AND version = '2'
);

INSERT INTO cron.job (schedule, command)
VALUES ('19 3 * * *', $$
    SELECT dex.insert_liquidity_bancor_v2(
        (SELECT max(day) FROM dex.liquidity WHERE project = 'Bancor' and version = '2'),
        (SELECT now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;