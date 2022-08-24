CREATE OR REPLACE FUNCTION dex.insert_dodoex_polygon(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
            token_a_amount_raw / 10 ^ (CASE token_a_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE pa.decimals END) * (CASE token_a_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN pe.price ELSE pa.price END),
            token_b_amount_raw / 10 ^ (CASE token_b_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE pb.decimals END) * (CASE token_b_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN pe.price ELSE pb.price END)
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

        -- dodo v1 sell
        SELECT
            s.evt_block_time AS block_time,
            'DODOEX' AS project,
            'v1_polygon' AS version,
            'DEX' AS category,
            s.seller AS trader_a,
            NULL::bytea AS trader_b,
            s."payBase" token_a_amount_raw,
            s."receiveQuote" token_b_amount_raw,
            NULL::numeric AS usd_amount,
            m.base_token_address AS token_a_address,
            m.quote_token_address AS token_b_address,
            s.contract_address exchange_contract_address,
            s.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            s.evt_index
        FROM
            dodoex."DODO_evt_SellBaseToken" s
        LEFT JOIN dodoex."view_markets_polygon" m on s.contract_address = m.market_contract_address
        WHERE s.seller <> '\x813fddeccd0401c4fa73b092b074802440544e52'
            AND s.evt_block_time >= start_ts
            AND s.evt_block_time < end_ts

        UNION ALL

        -- dodo v1 buy
        SELECT
            b.evt_block_time AS block_time,
            'DODOEX' AS project,
            'v1_polygon' AS version,
            'DEX' AS category,
            b.buyer AS trader_a,
            NULL::bytea AS trader_b,
            b."receiveBase" token_a_amount_raw,
            b."payQuote" token_b_amount_raw,
            NULL::numeric AS usd_amount,
            m.base_token_address AS token_a_address,
            m.quote_token_address AS token_b_address,
            b.contract_address exchange_contract_address,
            b.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            b.evt_index
        FROM
            dodoex."DODO_evt_BuyBaseToken" b
        LEFT JOIN dodoex."view_markets_polygon" m on b.contract_address = m.market_contract_address
        WHERE b.buyer <> '\x813fddeccd0401c4fa73b092b074802440544e52'
            AND b.evt_block_time >= start_ts
            AND b.evt_block_time < end_ts

        UNION ALL

        -- DODORouteProxy
        SELECT
            evt_block_time AS block_time,
            'DODOEX' AS project,
            'route_polygon' AS version,
            'Aggregator' AS category,
            sender AS trader_a,
            NULL::bytea AS trader_b,
            "fromAmount" token_a_amount_raw,
            "returnAmount" token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "fromToken" AS token_a_address,
            "toToken" AS token_b_address,
            contract_address exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM
            dodo_v2."DODORouteProxy_evt_OrderHistory"
        WHERE evt_block_time >= start_ts
            AND evt_block_time < end_ts

        UNION ALL

        -- DODOV2Proxy02
        SELECT
            evt_block_time AS block_time,
            'DODOEX' AS project,
            'v2_polygon' AS version,
            'Aggregator' AS category,
            sender AS trader_a,
            NULL::bytea AS trader_b,
            "fromAmount" token_a_amount_raw,
            "returnAmount" token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "fromToken" AS token_a_address,
            "toToken" AS token_b_address,
            contract_address exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM
            dodo_v2."DODOV2Proxy02_evt_OrderHistory"
        WHERE evt_block_time >= start_ts
            AND evt_block_time < end_ts

        UNION ALL

        -- dodov2 dvm
        SELECT
            evt_block_time AS block_time,
            'DODOEX' AS project,
            'v2_polygon' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            "fromAmount" token_a_amount_raw,
            "toAmount" token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "fromToken" AS token_a_address,
            "toToken" AS token_b_address,
            contract_address exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM
            dodoex."DVM_evt_DODOSwap"
        WHERE trader <> '\x813fddeccd0401c4fa73b092b074802440544e52'
            AND evt_block_time >= start_ts
            AND evt_block_time < end_ts

        UNION ALL

        -- dodov2 dpp
        SELECT
            evt_block_time AS block_time,
            'DODOEX' AS project,
            'v2_polygon' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            "fromAmount" AS token_a_amount_raw,
            "toAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "fromToken" AS token_a_address,
            "toToken" AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM
            dodoex."DPPOracle_evt_DODOSwap"
        WHERE trader <> '\x813fddeccd0401c4fa73b092b074802440544e52'
            AND evt_block_time >= start_ts
            AND evt_block_time < end_ts

        UNION ALL

        -- dodov2 dsp
        SELECT
            evt_block_time AS block_time,
            'DODOEX' AS project,
            'v2_polygon' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            "fromAmount" AS token_a_amount_raw,
            "toAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "fromToken" AS token_a_address,
            "toToken" AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM
            dodoex."DSP_evt_DODOSwap"
        WHERE trader <> '\x813fddeccd0401c4fa73b092b074802440544e52'
            AND evt_block_time >= start_ts
            AND evt_block_time < end_ts
    ) dexs
    INNER JOIN polygon.transactions tx
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
    -- LEFT JOIN prices.layer1_usd pe ON pe.minute = date_trunc('minute', dexs.block_time)
    --     AND pe.symbol = 'MATIC'
    LEFT JOIN prices.usd pe ON pe.minute = date_trunc('minute', dexs.block_time)
        AND pe.symbol = 'WMATIC'
        AND pe.minute >= start_ts
        AND pe.minute < end_ts
    WHERE dexs.block_time >= start_ts
    AND dexs.block_time < end_ts
    AND dexs.token_a_address <> dexs.token_b_address

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2020
SELECT dex.insert_dodoex_polygon(
    '2020-09-01',
    '2021-01-01',
    (SELECT max(number) FROM polygon.blocks WHERE time < '2020-09-01'),
    (SELECT max(number) FROM polygon.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2020-09-01'
    AND block_time <= '2021-01-01'
    AND project = 'DODOEX'
);

-- fill 2021
SELECT dex.insert_dodoex_polygon(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM polygon.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM polygon.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'DODOEX'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_dodoex_polygon(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='DODOEX'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM polygon.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='DODOEX')),
        (SELECT MAX(number) FROM polygon.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
