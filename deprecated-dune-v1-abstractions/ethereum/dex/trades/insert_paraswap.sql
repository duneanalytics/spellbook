CREATE OR REPLACE FUNCTION dex.insert_paraswap(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO dex.trades (
        block_time,
        token_a_symbol,
        token_b_symbol,
        token_a_amount,
        token_b_amount,
        project,
        version,
        category,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index,
        trade_id
    )
    SELECT
        dexs.block_time,
        erc20a.symbol AS token_a_symbol,
        erc20b.symbol AS token_b_symbol,
        token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
        token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
        'Paraswap' AS project,
        '1' AS version,
        'Aggregator' AS category,
        COALESCE(trader_a, tx."from") AS trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        GREATEST(
            token_a_amount_raw / 10 ^ pa.decimals * pa.price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.price
        ) AS usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx."from" AS tx_from,
        tx."to" as tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY tx_hash, evt_index, trace_address) AS trade_id

    FROM (

        -- AugustusSwapper_evt_Swapped
        SELECT
            swaps."evt_block_time" AS block_time,
            swaps."user" AS trader_a,
            NULL::bytea AS trader_b,
            swaps."srcAmount" AS token_a_amount_raw,
            swaps."receivedAmount" AS token_b_amount_raw,
            swaps."srcToken" AS token_a_address,
            swaps."destToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."evt_tx_hash" AS tx_hash,
            NULL::integer[] AS trace_address,
            swaps."evt_index" AS evt_index
        FROM paraswap."AugustusSwapper_evt_Swapped" swaps
        UNION ALL

        -- AugustusSwapper1.0_evt_Swapped
        SELECT
            swaps."evt_block_time" AS block_time,
            swaps."user" AS trader_a,
            NULL::bytea AS trader_b,
            swaps."srcAmount" AS token_a_amount_raw,
            swaps."receivedAmount" AS token_b_amount_raw,
            swaps."srcToken" AS token_a_address,
            swaps."destToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."evt_tx_hash" AS tx_hash,
            NULL::integer[] AS trace_address,
            swaps."evt_index" AS evt_index
        FROM paraswap."AugustusSwapper1.0_evt_Swapped" swaps
        UNION ALL

        -- AugustusSwapper2.0_evt_Swapped
        -- AugustusSwapper3.0_evt_Swapped
        -- AugustusSwapper4.0_evt_Swapped
        -- AugustusSwapper5.0_evt_Swapped
        -- AugustusSwapper3.0_evt_Bought
        SELECT
            swaps."evt_block_time" AS block_time,
            swaps."initiator" AS trader_a,
            swaps."beneficiary" AS trader_b,
            swaps."srcAmount" AS token_a_amount_raw,
            swaps."receivedAmount" AS token_b_amount_raw,
            swaps."srcToken" AS token_a_address,
            swaps."destToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."evt_tx_hash" AS tx_hash,
            NULL::integer[] AS trace_address,
            swaps."evt_index" AS evt_index
        FROM(
            SELECT * FROM paraswap."AugustusSwapper2.0_evt_Swapped" UNION ALL
            SELECT * FROM paraswap."AugustusSwapper3.0_evt_Swapped" UNION ALL
            SELECT * FROM paraswap."AugustusSwapper4.0_evt_Swapped" UNION ALL
            SELECT * FROM paraswap."AugustusSwapper5.0_evt_Swapped" UNION ALL
            SELECT * FROM paraswap."AugustusSwapper3.0_evt_Bought"
        ) swaps
        UNION ALL

        -- AugustusSwapper4.0_evt_Bought 
        -- AugustusSwapper5.0_evt_Bought
        SELECT
            swaps."evt_block_time" AS block_time,
            swaps."initiator" AS trader_a,
            swaps."beneficiary" AS trader_b,
            swaps."srcAmount" AS token_a_amount_raw,
            swaps."receivedAmount" AS token_b_amount_raw,
            swaps."srcToken" AS token_a_address,
            swaps."destToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."evt_tx_hash" AS tx_hash,
            NULL::integer[] AS trace_address,
            swaps."evt_index" AS evt_index
        FROM (
            SELECT * FROM paraswap."AugustusSwapper4.0_evt_Bought" UNION ALL
            SELECT * FROM paraswap."AugustusSwapper5.0_evt_Bought"
        ) swaps
        UNION ALL

        -- AugustusSwapper5.0_call_swapOnUniswap
        -- AugustusSwapper5.0_call_buyOnUniswap
        SELECT
            swaps."call_block_time" AS block_time,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            swaps."amountIn" AS token_a_amount_raw,
            swaps."amountOutMin" AS token_b_amount_raw,
            SUBSTRING(swaps."path"::text, 4, 42)::bytea AS token_a_address,
            LEFT(RIGHT(swaps."path"::text, 44), 42)::bytea AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."call_tx_hash" AS tx_hash,
            swaps."call_trace_address" AS trace_address,
            NULL::int8 AS evt_index
        FROM (
            SELECT *
            FROM paraswap."AugustusSwapper5.0_call_swapOnUniswap"
            WHERE "call_success" = true
            UNION ALL
            SELECT *
            FROM paraswap."AugustusSwapper5.0_call_buyOnUniswap"
            WHERE "call_success" = true
        ) swaps
        UNION ALL

        -- AugustusSwapper5.0_call_swapOnUniswapFork
        -- AugustusSwapper5.0_call_buyOnUniswapFork
        SELECT
            swaps."call_block_time" AS block_time,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            swaps."amountIn" AS token_a_amount_raw,
            swaps."amountOutMin" AS token_b_amount_raw,
            SUBSTRING(swaps."path"::text, 4, 42)::bytea AS token_a_address,
            LEFT(RIGHT(swaps."path"::text, 44), 42)::bytea AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."call_tx_hash" AS tx_hash,
            swaps."call_trace_address" AS trace_address,
            NULL::int8 AS evt_index
        FROM (
            SELECT *
            FROM paraswap."AugustusSwapper5.0_call_swapOnUniswapFork"
            WHERE "call_success" = true
            UNION ALL
            SELECT *
            FROM paraswap."AugustusSwapper5.0_call_buyOnUniswapFork"
            WHERE "call_success" = true
        ) swaps
        UNION ALL
        
        -- AugustusSwapper6.0_evt_Swapped
        -- AugustusSwapper6.0_evt_Swapped2
        SELECT 
            swaps."evt_block_time" AS block_time,
            swaps."initiator"::bytea AS trader_a,
            swaps."beneficiary"::bytea AS trader_b,
            swaps."srcAmount" AS token_a_amount_raw,
            swaps."receivedAmount" AS token_b_amount_raw,
            swaps."srcToken" AS token_a_address,
            swaps."destToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."evt_tx_hash" AS tx_hash,
            NULL::integer[] AS trace_address,
            swaps."evt_index" AS evt_index
        FROM (
            SELECT "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index"
                FROM paraswap."AugustusSwapper6.0_evt_Swapped"
            UNION ALL
            SELECT "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index"
                FROM paraswap."AugustusSwapper6.0_evt_Swapped2"
            UNION ALL
            SELECT "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index"
                FROM paraswap."AugustusSwapper6.0_evt_SwappedV3"
        ) swaps
        UNION ALL

        -- AugustusSwapper6.0_evt_Bought
        -- AugustusSwapper6.0_evt_Bought2
        SELECT 
            swaps."evt_block_time" AS block_time,
            swaps."initiator"::bytea AS trader_a,
            swaps."beneficiary"::bytea AS trader_b,
            swaps."srcAmount" AS token_a_amount_raw,
            swaps."receivedAmount" AS token_b_amount_raw,
            swaps."srcToken" AS token_a_address,
            swaps."destToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."evt_tx_hash" AS tx_hash,
            NULL::integer[] AS trace_address,
            swaps."evt_index" AS evt_index
        FROM (
            SELECT "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index"
                FROM paraswap."AugustusSwapper6.0_evt_Bought"
            UNION ALL
            SELECT "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index"
                FROM paraswap."AugustusSwapper6.0_evt_Bought2"
            UNION ALL
            SELECT "evt_block_time", "initiator", "beneficiary", "srcAmount", "receivedAmount", "srcToken", "destToken", "contract_address", "evt_tx_hash", "evt_index"
                FROM paraswap."AugustusSwapper6.0_evt_BoughtV3"
        ) swaps
        UNION ALL

        -- AugustusSwapper6.0_call_swapOnUniswap
        -- AugustusSwapper6.0_call_buyOnUniswap
        SELECT 
            swaps."call_block_time" AS block_time,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            swaps."amountIn" AS token_a_amount_raw,
            swaps."amountOutMin" AS token_b_amount_raw,
            SUBSTRING(swaps."path"::text, 4, 42)::bytea AS token_a_address,
            LEFT(RIGHT(swaps."path"::text, 44), 42)::bytea AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."call_tx_hash" AS tx_hash,
            swaps."call_trace_address" AS trace_address,
            NULL::int8 AS evt_index
        FROM (
            SELECT *
            FROM paraswap."AugustusSwapper6.0_call_swapOnUniswap"
            WHERE "call_success" = true
            UNION ALL
            SELECT *
            FROM paraswap."AugustusSwapper6.0_call_buyOnUniswap"
            WHERE "call_success" = true
        ) swaps
        UNION ALL

        -- AugustusSwapper6.0_call_swapOnUniswapFork
        -- AugustusSwapper6.0_call_buyOnUniswapFork
        SELECT 
            swaps."call_block_time" AS block_time,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            swaps."amountIn" AS token_a_amount_raw,
            swaps."amountOutMin" AS token_b_amount_raw,
            SUBSTRING(swaps."path"::text, 4, 42)::bytea AS token_a_address,
            LEFT(RIGHT(swaps."path"::text, 44), 42)::bytea AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."call_tx_hash" AS tx_hash,
            swaps."call_trace_address" AS trace_address,
            NULL::int8 AS evt_index
        FROM (
            SELECT *
            FROM paraswap."AugustusSwapper6.0_call_swapOnUniswapFork"
            WHERE "call_success" = true
            UNION ALL
            SELECT *
            FROM paraswap."AugustusSwapper6.0_call_buyOnUniswapFork"
            WHERE "call_success" = true
        ) swaps
        UNION ALL

        -- AugustusSwapper6.0_call_swapOnUniswapV2Fork
        -- AugustusSwapper6.0_call_buyOnUniswapV2Fork
        SELECT 
            swaps."call_block_time" AS block_time,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            swaps."amountIn" AS token_a_amount_raw,
            swaps."amountOutMin" AS token_b_amount_raw,
            swaps."tokenIn" AS token_a_address,
            NULL::bytea AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."call_tx_hash" AS tx_hash,
            swaps."call_trace_address" AS trace_address,
            NULL::int8 AS evt_index
        FROM (
            SELECT *
            FROM paraswap."AugustusSwapper6.0_call_swapOnUniswapV2Fork"
            WHERE "call_success" = true
            UNION ALL
            SELECT *
            FROM paraswap."AugustusSwapper6.0_call_buyOnUniswapV2Fork"
            WHERE "call_success" = true
        ) swaps
        UNION ALL

        -- AugustusSwapper6.0_call_swapOnUniswapV2ForkWithPermit
        -- AugustusSwapper6.0_call_buyOnUniswapV2ForkWithPermit
        SELECT 
            swaps."call_block_time" AS block_time,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            swaps."amountIn" AS token_a_amount_raw,
            swaps."amountOutMin" AS token_b_amount_raw,
            swaps."tokenIn" AS token_a_address,
            NULL::bytea AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."call_tx_hash" AS tx_hash,
            swaps."call_trace_address" AS trace_address,
            NULL::int8 AS evt_index
        FROM (
            SELECT *
            FROM paraswap."AugustusSwapper6.0_call_swapOnUniswapV2ForkWithPermit"
            WHERE "call_success" = true
            UNION ALL
            SELECT *
            FROM paraswap."AugustusSwapper6.0_call_buyOnUniswapV2ForkWithPermit"
            WHERE "call_success" = true
        ) swaps
        UNION ALL

        -- AugustusSwapper6.0_call_swapOnZeroXv2 
        -- AugustusSwapper6.0_call_swapOnZeroXv4
        SELECT
            swaps."call_block_time" AS block_time,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            swaps."fromAmount" AS token_a_amount_raw,
            swaps."amountOutMin" AS token_b_amount_raw,
            swaps."fromToken" AS token_a_address,
            swaps."toToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."call_tx_hash" AS tx_hash,
            swaps."call_trace_address" AS trace_address,
            NULL::int8 AS evt_index
        FROM (
            SELECT * FROM paraswap."AugustusSwapper6.0_call_swapOnZeroXv2" UNION ALL
            SELECT * FROM paraswap."AugustusSwapper6.0_call_swapOnZeroXv4"
        ) swaps
        WHERE swaps."call_success" = true
        UNION ALL

        -- AugustusSwapper6.0_call_swapOnZeroXv2WithPermit
        -- AugustusSwapper6.0_call_swapOnZeroXv4WithPermit
        SELECT
            swaps."call_block_time" AS block_time,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            swaps."fromAmount" AS token_a_amount_raw,
            swaps."amountOutMin" AS token_b_amount_raw,
            swaps."fromToken" AS token_a_address,
            swaps."toToken" AS token_b_address,
            swaps."contract_address" AS exchange_contract_address,
            swaps."call_tx_hash" AS tx_hash,
            swaps."call_trace_address" AS trace_address,
            NULL::int8 AS evt_index
        FROM (
            SELECT * FROM paraswap."AugustusSwapper6.0_call_swapOnZeroXv2WithPermit" UNION ALL
            SELECT * FROM paraswap."AugustusSwapper6.0_call_swapOnZeroXv4WithPermit"
        ) swaps
        WHERE swaps."call_success" = true

    ) dexs

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
    WHERE dexs.block_time >= start_ts
    AND dexs.block_time < end_ts

    ON CONFLICT DO NOTHING
    RETURNING 1

)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2019
SELECT dex.insert_paraswap(
    '2019-11-01',
    '2020-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-11-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2019-11-01'
    AND block_time <= '2020-01-01'
    AND project = 'Paraswap'
);

-- fill 2020
SELECT dex.insert_paraswap(
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
    AND project = 'Paraswap'
);

-- fill 2021
SELECT dex.insert_paraswap(
    '2021-01-01',
    '2022-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2022-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= '2022-01-01'
    AND project = 'Paraswap'
);

-- fill 2022
SELECT dex.insert_paraswap(
    '2022-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2022-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2022-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Paraswap'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_paraswap(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Paraswap'),
        (SELECT now()),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Paraswap')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
