--###### Forked from Ethreum insert_1inch at https://github.com/duneanalytics/abstractions/blob/master/ethereum/dex/trades/insert_1inch.sql

CREATE OR REPLACE FUNCTION dex.insert_oneinch(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
        bep20a.symbol AS token_a_symbol,
        bep20b.symbol AS token_b_symbol,
        token_a_amount_raw / 10 ^ bep20a.decimals AS token_a_amount,
        token_b_amount_raw / 10 ^ bep20b.decimals AS token_b_amount,
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
            oi.block_time,
            '1inch' AS project,
            version,
            'Aggregator' AS category,
            tx."from" AS trader_a,
            NULL::bytea AS trader_b,
            to_amount AS token_a_amount_raw,
            from_amount AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            (CASE WHEN to_token = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' ELSE to_token END) AS token_a_address,
            (CASE WHEN from_token = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' ELSE from_token END) AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            trace_address,
            evt_index
        FROM (
            SELECT "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as trace_address, evt_index, contract_address, '3' as version FROM oneinch."OneInchExchange_evt_Swapped" WHERE evt_block_time >= start_ts AND evt_block_time < end_ts UNION ALL
            SELECT decode(substring("desc"->>'srcToken' FROM 3), 'hex') as from_token, decode(substring("desc"->>'dstToken' FROM 3), 'hex') as to_token, ("desc"->>'amount')::numeric as from_amount, "output_returnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address, NULL::integer as evt_index, contract_address, '4' as version FROM oneinch."AggregationRouterV4_call_swap" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts
        ) oi
        left join bsc."transactions" tx on hash = tx_hash
            AND tx.block_time >= start_ts
            AND tx.block_time < end_ts

        UNION ALL

        -- 1inch Unoswap
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            'UNI v2' AS version,
            'Aggregator' AS category,
            tx."from" AS trader_a,
            NULL::bytea AS trader_b,
            "output_returnAmount" AS token_a_amount_raw,
            "amount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            ll.to AS token_a_address,
            (CASE WHEN "srcToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' ELSE "srcToken" END) AS token_b_address,
            us.contract_address AS exchange_contract_address,
            call_tx_hash,
            call_trace_address AS trace_address,
            NULL::integer AS evt_index
        FROM (
            select "output_returnAmount", "amount", "srcToken", "_3" as pools, "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch."AggregationRouterV3_call_unoswap" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select "output_returnAmount", "amount", "srcToken", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch."AggregationRouterV3_call_unoswapWithPermit" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts
            UNION ALL
            select "output_returnAmount", "amount", "srcToken", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch."AggregationRouterV4_call_unoswap"  where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select "output_returnAmount", "amount", "srcToken", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch."AggregationRouterV4_call_unoswapWithPermit"  where call_success AND call_block_time >= start_ts AND call_block_time < end_ts
        ) us
        LEFT JOIN bsc."transactions" tx ON tx.hash = us.call_tx_hash
            AND tx.block_time >= start_ts
            AND tx.block_time < end_ts
        LEFT JOIN bsc.traces tr ON tr.tx_hash = us.call_tx_hash AND tr.trace_address = us.call_trace_address[:ARRAY_LENGTH(us.call_trace_address, 1)-1]
            AND tr.block_time >= start_ts
            AND tr.block_time < end_ts
        LEFT JOIN bsc.traces ll ON ll.tx_hash = us.call_tx_hash AND ll.trace_address = (us.call_trace_address || (ARRAY_LENGTH("pools", 1)*2 + CASE WHEN "srcToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 1 ELSE 0 END) || 0)
            AND ll.block_time >= start_ts
            AND ll.block_time < end_ts
        
        -- UNION ALL

        -- 1inch Uniswap V3 Router --No TX so far
        -- SELECT
        --     call_block_time as block_time,
        --     '1inch' AS project,
        --     'UNI v3' AS version,
        --     'Aggregator' AS category,
        --     tx."from" AS trader_a,
        --     NULL::bytea AS trader_b,
        --     "output_returnAmount" AS token_a_amount_raw,
        --     "amount" AS token_b_amount_raw,
        --     NULL::numeric AS usd_amount,
        --     "dstToken" AS token_a_address,
        --     "srcToken" AS token_b_address,
        --     us.contract_address AS exchange_contract_address,
        --     call_tx_hash,
        --     call_trace_address AS trace_address,
        --     NULL::integer AS evt_index
        -- FROM (
        --     select 
        --         "output_returnAmount", "amount",
        --         COALESCE((select tr1.to from
        --             bsc.traces tr1 where call_type = 'call' and tr1.tx_hash = call_tx_hash and substring(tr1.input from 1 for 4) = '\x23b872dd' --To add UniswapV3 this function needs to be checked
        --             and COALESCE(call_trace_address, array[]::int[]) = tr1.trace_address[:COALESCE(ARRAY_LENGTH(call_trace_address, 1), 0)]
        --             and COALESCE(ARRAY_LENGTH(call_trace_address, 1), 0) + 3 = COALESCE(ARRAY_LENGTH(tr1.trace_address, 1), 0)
        --             order by COALESCE(trace_address, array[]::int[])
        --             LIMIT 1
        --         ), '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c') as "srcToken",
        --         CASE WHEN ((pools[array_length(pools, 1)] / 2^252)::int & 2 <> 0) THEN '\xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
        --         ELSE
        --             (select tr2.to from
        --                 bsc.traces tr2 where call_type = 'call' and tr2.tx_hash = call_tx_hash and substring(tr2.input from 1 for 4) = '\xa9059cbb' --To add UniswapV3 this function needs to be checked
        --                 and COALESCE(call_trace_address, array[]::int[]) = tr2.trace_address[:COALESCE(ARRAY_LENGTH(call_trace_address, 1), 0)]
        --                 and COALESCE(ARRAY_LENGTH(call_trace_address, 1), 0) + 2 = COALESCE(ARRAY_LENGTH(tr2.trace_address, 1), 0)
        --                 and tr2.from <> contract_address
        --                 order by COALESCE(trace_address, array[]::int[]) desc
        --                 LIMIT 1
        --             )
        --         END as "dstToken",
        --         "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address"
        --     from (
        --         select "output_returnAmount", "amount", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch."AggregationRouterV4_call_uniswapV3Swap" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
        --         select "output_returnAmount", "amount", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch."AggregationRouterV4_call_uniswapV3SwapTo" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
        --         select "output_returnAmount", "amount", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch."AggregationRouterV4_call_uniswapV3SwapToWithPermit" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts
        --     ) sw
        -- ) us
        -- LEFT JOIN bsc."transactions" tx ON tx.hash = us.call_tx_hash
        --     AND tx.block_time >= start_ts
        --     AND tx.block_time < end_ts
        
        UNION ALL

        -- 1inch Limit Order Protocol
        SELECT
            call_block_time as block_time,
            '1inch Limit Order Protocol' AS project,
            version,
            'DEX' AS category,
            "from"  AS trader_a,
            maker AS trader_b,
            "output_1" AS token_a_amount_raw,
            "output_0" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            decode(substring("order"::jsonb->>'takerAsset' from 3), 'hex') AS token_a_address,
            decode(substring("order"::jsonb->>'makerAsset' from 3), 'hex') AS token_b_address,
            contract_address AS exchange_contract_address,
            call_tx_hash,
            call_trace_address,
            NULL AS evt_index
        FROM (
            select '2' as version, decode(substring("order"::jsonb->>'maker' from 3 for 40), 'hex') as maker, contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch."AggregationRouterV4_call_fillOrderRFQ" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select '2' as version, decode(substring("order"::jsonb->>'maker' from 3 for 40), 'hex') as maker, contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch."AggregationRouterV4_call_fillOrderRFQTo" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select '2' as version, decode(substring("order"::jsonb->>'maker' from 3 for 40), 'hex') as maker, contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch."AggregationRouterV4_call_fillOrderRFQToWithPermit" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts
        ) call
        LEFT JOIN bsc.traces ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            AND ts.block_time >= start_ts
            AND ts.block_time < end_ts
        
        UNION ALL

        -- 1inch Limit Order Protocol Embedded RFQ v1
        SELECT
            call_block_time as block_time,
            '1inch Limit Order Protocol' AS project,
            'eRFQ v1' AS version,
            'DEX' AS category,
            "from"  AS trader_a,
            decode(substring("order"::jsonb->>'maker' from 3), 'hex') AS trader_b,
            "output_1" AS token_a_amount_raw,
            "output_0" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            decode(substring("order"::jsonb->>'takerAsset' from 3), 'hex') AS token_a_address,
            decode(substring("order"::jsonb->>'makerAsset' from 3), 'hex') AS token_b_address,
            contract_address AS exchange_contract_address,
            call_tx_hash,
            trace_address,
            NULL AS evt_index
        FROM (
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch."AggregationRouterV4_call_fillOrderRFQ" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch."AggregationRouterV4_call_fillOrderRFQTo" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch."AggregationRouterV4_call_fillOrderRFQToWithPermit" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts
        ) tt
        LEFT JOIN bsc.traces ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            AND ts.block_time >= start_ts
            AND ts.block_time < end_ts
        
        UNION ALL

        -- 1inch Embedded RFQ v1
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            'eRFQ v1' AS version,
            'Aggregator' AS category,
            "from"  AS trader_a,
            decode(substring("order"::jsonb->>'maker' from 3), 'hex') AS trader_b,
            "output_1" AS token_a_amount_raw,
            "output_0" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            decode(substring("order"::jsonb->>'takerAsset' from 3), 'hex') AS token_a_address,
            decode(substring("order"::jsonb->>'makerAsset' from 3), 'hex') AS token_b_address,
            contract_address AS exchange_contract_address,
            call_tx_hash,
            trace_address,
            NULL AS evt_index
        FROM (
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch."AggregationRouterV4_call_fillOrderRFQ" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch."AggregationRouterV4_call_fillOrderRFQTo" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch."AggregationRouterV4_call_fillOrderRFQToWithPermit" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts
        ) tt
        LEFT JOIN bsc.traces ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            AND ts.block_time >= start_ts
            AND ts.block_time < end_ts

        UNION ALL
        
        -- 1inch Limit Order Protocol RFQ v2
        SELECT
            call_block_time as block_time,
            '1inch Limit Order Protocol' AS project,
            'RFQ v2' as version,
            'DEX' AS category,
            ts."from" AS trader_a,
            decode(substring("order"::jsonb->>'maker' from 3 for 40), 'hex') AS trader_b,
            "output_1" AS token_a_amount_raw,
            "output_0" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            decode(substring("order"::jsonb->>'takerAsset' from 3), 'hex') AS token_a_address,
            decode(substring("order"::jsonb->>'makerAsset' from 3), 'hex') AS token_b_address,
            contract_address AS exchange_contract_address,
            call_tx_hash,
            call_trace_address,
            NULL AS evt_index
        FROM (
            select contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch."AggregationRouterV4_call_fillOrderRFQ" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch."AggregationRouterV4_call_fillOrderRFQTo" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts union all
            select contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch."AggregationRouterV4_call_fillOrderRFQToWithPermit" where call_success AND call_block_time >= start_ts AND call_block_time < end_ts
        ) call
        LEFT JOIN bsc.traces ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            AND ts.block_time >= start_ts
            AND ts.block_time < end_ts
    ) dexs

    INNER JOIN bsc."transactions" tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= start_ts 
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN bep20.tokens bep20a ON bep20a.contract_address = dexs.token_a_address
    LEFT JOIN bep20.tokens bep20b ON bep20b.contract_address = dexs.token_b_address
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

-- fill 2021
SELECT dex.insert_oneinch(
    '2021-01-01',
    '2022-01-01',
    (SELECT max(number) FROM bsc.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM bsc.blocks where time < '2022-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= '2022-01-01'
);

-- fill 2022
SELECT dex.insert_oneinch(
    '2022-01-01',
    now(),
    (SELECT max(number) FROM bsc.blocks WHERE time < '2022-01-01'),
    (SELECT MAX(number) FROM bsc.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2022-01-01'
    AND block_time <= now() - interval '20 minutes'
);


INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_oneinch(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE (project='1inch' OR project='1inch Limit Order Protocol')),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM bsc.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE (project='1inch'OR project='1inch Limit Order Protocol'))),
        (SELECT MAX(number) FROM bsc.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
