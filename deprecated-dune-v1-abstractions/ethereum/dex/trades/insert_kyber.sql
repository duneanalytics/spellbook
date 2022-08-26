CREATE OR REPLACE FUNCTION dex.insert_kyber(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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

        SELECT
            evt_block_time AS block_time,
            'Kyber' AS project,
            '1' AS version,
            'DEX' AS category,
            trader AS trader_a,
            NULL::bytea AS trader_b,
            CASE
                WHEN src IN ('\x5228a22e72ccc52d415ecfd199f99d0665e7733b') THEN 0 -- ignore volume of token PT
                ELSE "srcAmount"
            END AS token_a_amount_raw,
            "ethWeiValue" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            src AS token_a_address,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_b_address,
            contract_address exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM
            kyber."Network_evt_KyberTrade"
        WHERE src NOT IN ('\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
        AND evt_block_time >= start_ts AND evt_block_time < end_ts
        
        UNION ALL

        -- Kyber: trade from ETH - Token
        SELECT

            evt_block_time AS block_time,
            'Kyber' AS project,
            '1' AS version,
            'DEX' AS category,
            trader AS trader_a,
            NULL::bytea AS trader_b,
            "ethWeiValue" AS token_a_amount_raw,
            CASE
                WHEN dest IN ('\x5228a22e72ccc52d415ecfd199f99d0665e7733b') THEN 0 -- ignore volume of token PT
                ELSE "dstAmount"
            END AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_a_address,
            dest AS token_b_address,
            contract_address exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM
            kyber."Network_evt_KyberTrade"
        WHERE dest NOT IN ('\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
        AND evt_block_time >= start_ts AND evt_block_time < end_ts
        
        UNION ALL

        --- Kyber_V2
        -- trade from token -eth
        SELECT
            evt_block_time AS block_time,
            'Kyber' AS project,
            '2' AS version,
            'DEX' AS category,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            (SELECT SUM(s) FROM UNNEST(ARRAY((
                -- formula: https://github.com/KyberNetwork/smart-contracts/blob/Katalyst/contracts/sol6/utils/Utils5.sol#L88
                SELECT
                    CASE WHEN 18 >= src_token.decimals -- eth decimal
                        THEN a*b*power(10, (18-src_token.decimals))/1e18
                        ELSE (a*b) / (1e18 * power(10, (src_token.decimals - 18)))
                    END
                FROM unnest("t2eSrcAmounts", "t2eRates") AS t(a,b)
                ))) s
            ) AS token_a_amount_raw,
            (SELECT SUM(a) FROM UNNEST("t2eSrcAmounts") AS a) AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_a_address, -- trade from token - eth, dest should be weth
            src AS token_b_address,
            trade.contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM kyber_v2."Network_evt_KyberTrade" trade
        INNER JOIN erc20."tokens" src_token ON trade.src = src_token.contract_address
        AND src_token.contract_address != '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        AND evt_block_time >= start_ts AND evt_block_time < end_ts
        
        UNION ALL

        -- trade from eth - token
        SELECT
            evt_block_time AS block_time,
            'Kyber' AS project,
            '2' AS version,
            'DEX' AS category,
            NULL::bytea AS trader_a,
            NULL::bytea AS trader_b,
            (SELECT SUM(s) FROM UNNEST(ARRAY((
                -- formula: https://github.com/KyberNetwork/smart-contracts/blob/Katalyst/contracts/sol6/utils/Utils5.sol#L88
                SELECT
                    CASE WHEN dst_token.decimals >= 18 -- eth decimal
                        THEN a*b*power(10, (dst_token.decimals-18))/1e18
                        ELSE (a*b) / (1e18 * power(10, (18- dst_token.decimals)))
                    END
                FROM unnest("e2tSrcAmounts", "e2tRates") AS t(a,b)
                ))) s
            ) AS token_a_amount_raw,
            (SELECT SUM(a) FROM UNNEST("e2tSrcAmounts") AS a) AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            dest AS token_a_address,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_b_address, -- trade from eth - token, src should be weth
            trade.contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM kyber_v2."Network_evt_KyberTrade" trade
        INNER JOIN erc20."tokens" dst_token ON trade.dest = dst_token.contract_address
        AND dst_token.contract_address != '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        AND evt_block_time >= start_ts AND evt_block_time < end_ts

        UNION ALL

        SELECT
            t.evt_block_time AS block_time,
            'Kyber' AS project,
            'dmm' AS version,
            'DEX' AS category,
            t."to" AS trader_a,
            NULL::bytea AS trader_b,
            CASE WHEN "amount0Out" = 0 THEN "amount1Out" ELSE "amount0Out" END AS token_a_amount_raw,
            CASE WHEN "amount0In" = 0 OR "amount1Out" = 0 THEN "amount1In" ELSE "amount0In" END AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            CASE WHEN "amount0Out" = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
            CASE WHEN "amount0In" = 0 OR "amount1Out" = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            kyber."DMMPool_evt_Swap" t
        INNER JOIN kyber."DMMFactory_evt_PoolCreated" f ON f.pool = t.contract_address        
        AND t.evt_block_time >= start_ts AND t.evt_block_time < end_ts

        UNION ALL

        -- from Aggregator 
        SELECT
            evt_block_time AS block_time,
            'Kyber' AS project,
            'dmm' AS version,
            'Aggregator' AS category,
            sender AS trader_a,
            NULL::bytea AS trader_b,
            "spentAmount" token_a_amount_raw,
            "returnAmount" token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "srcToken" token_a_address,
            "dstToken" token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index AS evt_index
        FROM kyber."AggregationRouterV2_evt_Swapped"
        WHERE evt_block_time >= start_ts AND evt_block_time < end_ts
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
SELECT dex.insert_kyber(
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
    AND project = 'Kyber'
);

-- fill 2020
SELECT dex.insert_kyber(
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
    AND project = 'Kyber'
);

-- fill 2021
SELECT dex.insert_kyber(
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
    AND project = 'Kyber'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_kyber(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Kyber'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Kyber')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;