{{ config(
    schema = 'bancor_v1_ethereum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "bancor_v1",
                                \'["codingsh"]\') }}'
    )
}}
WITH dexs AS
(
    -- Bancor Network
    SELECT
        block_time,
        'Bancor Network' AS project,
        version::text AS version,
        'DEX' AS category,
        trader AS trader_a,
        NULL::bytea AS trader_b,
        target_token_amount_raw AS token_a_amount_raw,
        source_token_amount_raw AS token_b_amount_raw,
        cast(NULL as double) AS usd_amount,
        CASE WHEN target_token_address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
        ELSE target_token_address
        END AS token_a_address,
        CASE WHEN source_token_address = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
        ELSE source_token_address
        END AS token_b_address,
        contract_address AS exchange_contract_address,
        tx_hash,
        CAST(NULL AS ARRAY<INT>) AS trace_address,
        evt_index
    FROM bancornetwork.view_convert
    INNER JOIN ethereum.transactions tx
            ON dexs.tx_hash = tx.hash
            AND tx.block_time >= start_ts
            AND tx.block_time < end_ts
            AND tx.block_number >= start_block
            AND tx.block_number < end_block
    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN prices.usd pa ON pa.minute = date_trunc('minute', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        AND pa.minute >= start_ts
        AND pa.minute < end_ts
    LEFT JOIN prices.usd pb ON pb.minute = date_trunc('minute', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        AND pb.minute >= start_ts
        AND pb.minute < end_ts
    {% if is_incremental() %}
    AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)


-- fill 2018
SELECT dex.insert_bancor(
    '2018-01-01',
    '2019-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2018-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2019-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2018-01-01'
    AND block_time <= '2019-01-01'
    AND project = 'Bancor Network'
);

-- fill 2019
SELECT dex.insert_bancor(
    '2019-01-01',
    '2020-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2019-01-01'
    AND block_time <= '2020-01-01'
    AND project = 'Bancor Network'
);

-- fill 2020
SELECT dex.insert_bancor(
    '2020-01-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
    AND project = 'Bancor Network'
);

-- fill 2021
SELECT dex.insert_bancor(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Bancor Network'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_bancor(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Bancor Network'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Bancor Network')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

   