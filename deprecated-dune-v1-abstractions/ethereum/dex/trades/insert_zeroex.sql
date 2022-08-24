CREATE OR REPLACE FUNCTION dex.insert_zeroex(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
        project,
        version,
        category,
        coalesce(trader_a, tx."from") as trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        coalesce(
            usd_amount,
            token_a_amount_raw / 10 ^ pa.decimals * pa.price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.price
        ) as usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx."from" as tx_from,
        tx."to" as tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
        -- 0x v2.1
        SELECT
            evt_block_time AS block_time,
            '0x Native' AS project,
            '2.1' AS version,
            'DEX' AS category,
            "takerAddress" AS trader_a,
            "makerAddress" AS trader_b,
            "takerAssetFilledAmount" AS token_a_amount_raw,
            "makerAssetFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            substring("takerAssetData" for 20 from 17) AS token_a_address,
            substring("makerAssetData" for 20 from 17) AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex_v2."Exchange2.1_evt_Fill"

        UNION ALL

        -- 0x v3
        SELECT
            evt_block_time AS block_time,
            '0x Native' AS project,
            '3' AS version,
            'DEX' AS category,
            "takerAddress" AS trader_a,
            "makerAddress" AS trader_b,
            "takerAssetFilledAmount" AS token_a_amount_raw,
            "makerAssetFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            substring("takerAssetData" for 20 from 17) AS token_a_address,
            substring("makerAssetData" for 20 from 17) AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex_v3."Exchange_evt_Fill"

        UNION ALL

        -- 0x v4 limit orders
        SELECT
            evt_block_time AS block_time,
            '0x Native' AS project,
            '4' AS version,
            'DEX' AS category,
            taker AS trader_a,
            maker AS trader_b,
            "takerTokenFilledAmount" AS token_a_amount_raw,
            "makerTokenFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "takerToken" AS token_a_address,
            "makerToken" AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex."ExchangeProxy_evt_LimitOrderFilled"

        UNION ALL

        -- 0x v4 rfq orders
        SELECT
            evt_block_time AS block_time,
            '0x Native' AS project,
            '4' AS version,
            'DEX' AS category,
            taker AS trader_a,
            maker AS trader_b,
            "takerTokenFilledAmount" AS token_a_amount_raw,
            "makerTokenFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "takerToken" AS token_a_address,
            "makerToken" AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex."ExchangeProxy_evt_RfqOrderFilled"

        UNION ALL

        SELECT evt_block_time AS block_time,
                '0x Native' AS project,
                '4' AS version,
                'DEX' AS category,
                taker AS trader_a,
                maker AS trader_b,
                "takerTokenFilledAmount" AS token_a_amount_raw,
                "makerTokenFilledAmount" AS token_b_amount_raw,
                NULL::numeric AS usd_amount,
                "takerToken" AS token_a_address,
                "makerToken" AS token_b_address,
                contract_address AS exchange_contract_address,
                evt_tx_hash AS tx_hash,
                NULL::integer[] AS trace_address,
                evt_index
        FROM zeroex."ExchangeProxy_evt_OtcOrderFilled"

        UNION ALL
        -- 0x api
        SELECT
            block_time,
            '0x API' AS project,
            NULL AS version,
            'Aggregator' AS category,
            "taker" AS trader_a,
            "maker" AS trader_b,
            "taker_token_amount_raw" AS token_a_amount_raw,
            "maker_token_amount_raw" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            taker_token AS token_a_address,
            maker_token AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex."view_0x_api_fills"
        where swap_flag is TRUE

        UNION ALL

        -- Matcha
        SELECT
            block_time,
            'Matcha' AS project,
            NULL AS version,
            'Aggregator' AS category,
            "taker" AS trader_a,
            "maker" AS trader_b,
            "taker_token_amount_raw" AS token_a_amount_raw,
            "maker_token_amount_raw" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            taker_token AS token_a_address,
            maker_token AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex."view_0x_api_fills"
        where affiliate_address ='\x86003b044f70dac0abc80ac8957305b6370893ed'
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
SELECT dex.insert_zeroex(
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
    AND project IN ('0x Native', '0x API', 'Matcha')
);

-- fill 2020
SELECT dex.insert_zeroex(
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
    AND project IN ('0x Native', '0x API', 'Matcha')
);

-- fill 2021
SELECT dex.insert_zeroex(
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
    AND project IN ('0x Native', '0x API', 'Matcha')
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_zeroex(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project IN ('0x Native', '0x API', 'Matcha')),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project IN ('0x Native', '0x API', 'Matcha'))),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
