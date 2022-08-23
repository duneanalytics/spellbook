CREATE OR REPLACE FUNCTION dex.insert_1inch(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
        SELECT
            oi.block_time,
            '1inch' AS project,
            version,
            'Aggregator' AS category,
            trader_a,
            NULL::bytea AS trader_b,
            to_amount AS token_a_amount_raw,
            from_amount AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            (CASE WHEN to_token = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE to_token END) AS token_a_address,
            (CASE WHEN from_token = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE from_token END) AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            trace_address,
            evt_index
        FROM
        (
            (
            SELECT t."from" as trader_a, calls.*
                FROM (
                    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v1_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v2_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v3_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v4_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v5_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v6_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                    SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v7_call_swap" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                    SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."OneInchExchange_call_swap" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                    SELECT decode(substring("desc"->>'srcToken' FROM 3), 'hex') as from_token, decode(substring("desc"->>'dstToken' FROM 3), 'hex') as to_token, "output_spentAmount" as from_amount, "output_returnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address, NULL::integer as evt_index, contract_address, '4' as version FROM oneinch_v4."AggregationRouterV4_call_swap" where call_success and call_block_time >= start_ts AND call_block_time < end_ts
                ) calls
                LEFT JOIN ethereum.traces t on calls.tx_hash = t.tx_hash and calls.trace_address = t.trace_address
                and t.block_time >= start_ts
                and t.block_time < end_ts)

            UNION ALL
            SELECT sender as trader_a, "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address, '2' as version 
            FROM oneinch_v2."OneInchExchange_evt_Swapped"
            WHERE
                evt_block_time >= start_ts
                and evt_block_time < end_ts
            UNION ALL
            SELECT sender as trader_a, "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address, '3' as version
            FROM oneinch_v3."AggregationRouterV3_evt_Swapped"
            WHERE
                evt_block_time >= start_ts
                and evt_block_time < end_ts
        ) oi

        UNION ALL

        SELECT
            oi.block_time,
            '1inch' AS project,
            '1split' as version,
            'Aggregator' AS category,
            t."from" AS trader_a,
            NULL::bytea AS trader_b,
            to_amount AS token_a_amount_raw,
            from_amount AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            (CASE WHEN to_token = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE to_token END) AS token_a_address,
            (CASE WHEN from_token = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE from_token END) AS token_b_address,
            contract_address AS exchange_contract_address,
            oi.tx_hash,
            call_trace_address as trace_address,
            NULL::integer as evt_index
        FROM (
            SELECT "fromToken" AS from_token, "toToken" AS to_token, "amount" AS from_amount, "minReturn" AS to_amount, call_tx_hash AS tx_hash, call_trace_address, call_block_time AS block_time, contract_address FROM onesplit."OneSplit_call_swap" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
            SELECT "fromToken" AS from_token, "toToken" AS to_token, "amount" AS from_amount, "minReturn" AS to_amount, call_tx_hash AS tx_hash, call_trace_address, call_block_time AS block_time, contract_address FROM onesplit."OneSplit_call_goodSwap" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts
        ) oi
        left join ethereum.traces t on oi.tx_hash = t.tx_hash and oi.call_trace_address = t.trace_address
            and t.block_time >= start_ts
            and t.block_time < end_ts
        where oi.tx_hash not in (
            select tx_hash from (
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v1_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v2_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v3_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v4_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v5_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v6_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v7_call_swap" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."OneInchExchange_call_swap" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address, '2' as version 
                    FROM oneinch_v2."OneInchExchange_evt_Swapped" 
                    WHERE
                        evt_block_time >= start_ts
                        and evt_block_time < end_ts 
                UNION ALL
                SELECT "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address, '3' as version 
                    FROM oneinch_v3."AggregationRouterV3_evt_Swapped"
                    WHERE
                        evt_block_time >= start_ts
                        and evt_block_time < end_ts
                UNION ALL
                SELECT decode(substring("desc"->>'srcToken' FROM 3), 'hex') as from_token, decode(substring("desc"->>'dstToken' FROM 3), 'hex') as to_token, "output_spentAmount" as from_amount, "output_returnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address, NULL::integer as evt_index, contract_address, '4' as version FROM oneinch_v4."AggregationRouterV4_call_swap" where call_success and call_block_time >= start_ts AND call_block_time < end_ts
            ) calls
        )

        UNION ALL

        SELECT
            oi.block_time,
            '1inch' AS project,
            '1proto' as version,
            'Aggregator' AS category,
            tx."from" AS trader_a,
            NULL::bytea AS trader_b,
            to_amount AS token_a_amount_raw,
            from_amount AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            (CASE WHEN to_token = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE to_token END) AS token_a_address,
            (CASE WHEN from_token = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE from_token END) AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM (
            SELECT "fromToken" as from_token, "destToken" as to_token, "fromTokenAmount" as from_amount, "destTokenAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, contract_address, evt_index FROM oneproto."OneSplitAudit_evt_Swapped"
            WHERE
                evt_block_time >= start_ts
                and evt_block_time < end_ts
        ) oi
        left join ethereum.transactions tx on hash = tx_hash
            and tx.block_time >= start_ts
            and tx.block_time < end_ts
        where tx_hash not in (
            select tx_hash from (
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v1_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v2_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v3_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v4_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v5_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v6_call_aggregate" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."exchange_v7_call_swap" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address, '1' as version FROM oneinch."OneInchExchange_call_swap" WHERE call_success and call_block_time >= start_ts AND call_block_time < end_ts UNION ALL
                SELECT "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address, '2' as version FROM oneinch_v2."OneInchExchange_evt_Swapped" where evt_block_time >= start_ts AND evt_block_time < end_ts UNION ALL
                SELECT "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address, '3' as version FROM oneinch_v3."AggregationRouterV3_evt_Swapped" where evt_block_time >= start_ts AND evt_block_time < end_ts UNION ALL
                SELECT decode(substring("desc"->>'srcToken' FROM 3), 'hex') as from_token, decode(substring("desc"->>'dstToken' FROM 3), 'hex') as to_token, "output_spentAmount" as from_amount, "output_returnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address, NULL::integer as evt_index, contract_address, '4' as version FROM oneinch_v4."AggregationRouterV4_call_swap" where call_success and call_block_time >= start_ts AND call_block_time < end_ts
            ) t
        )
        
        UNION ALL

        -- 1inch 0x Limit Orders
        SELECT
            evt_block_time as block_time,
            '1inch' AS project,
            'ZRX' AS version,
            'Aggregator' AS category,
            "takerAddress" AS trader_a,
            "makerAddress" AS trader_b,
            "takerAssetFilledAmount" AS token_a_amount_raw,
            "makerAssetFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            substring("takerAssetData" for 20 from 17) AS token_a_address,
            substring("makerAssetData" for 20 from 17) AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM (
            select "feeRecipientAddress", "takerAssetData", "makerAssetData", "makerAddress", "takerAddress", "makerAssetFilledAmount", "takerAssetFilledAmount", contract_address, evt_block_time, evt_tx_hash, evt_index
            from zeroex_v2."Exchange2.0_evt_Fill" 
            where evt_block_time >= start_ts
                and evt_block_time < end_ts
            union all -- 0x v1
            select "feeRecipientAddress", "takerAssetData", "makerAssetData", "makerAddress", "takerAddress", "makerAssetFilledAmount", "takerAssetFilledAmount", contract_address, evt_block_time, evt_tx_hash, evt_index 
            from zeroex_v2."Exchange2.1_evt_Fill" 
            where evt_block_time >= start_ts
                and evt_block_time < end_ts
            union all -- 0x v2
            select "feeRecipientAddress", "takerAssetData", "makerAssetData", "makerAddress", "takerAddress", "makerAssetFilledAmount", "takerAssetFilledAmount", contract_address, evt_block_time, evt_tx_hash, evt_index 
            from zeroex_v3."Exchange_evt_Fill" 
            where evt_block_time >= start_ts
                and evt_block_time < end_ts
            union all -- 0x v3
            select "feeRecipient", "takerToken", "makerToken", "maker", "taker", "makerTokenFilledAmount", "takerTokenFilledAmount", contract_address, evt_block_time, evt_tx_hash, evt_index 
            from zeroex."ExchangeProxy_evt_LimitOrderFilled" -- 0x v4
            where evt_block_time >= start_ts
                and evt_block_time < end_ts
        ) oi
        WHERE "feeRecipientAddress" IN ('\x910bf2d50fa5e014fd06666f456182d4ab7c8bd2', '\x68a17b587caf4f9329f0e372e3a78d23a46de6b5')

        UNION ALL

        -- 1inch Unoswap
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            'UNI v2' AS version,
            'Aggregator' AS category,
            t."from" AS trader_a,
            NULL::bytea AS trader_b,
            "output_returnAmount" AS token_a_amount_raw,
            "amount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            (CASE WHEN ll.to = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND substring("pools"[ARRAY_LENGTH("pools", 1)] from 1 for 1) IN ('\xc0', '\x40') THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE ll.to END) AS token_a_address,
            (CASE WHEN "srcToken" = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE "srcToken" END) AS token_b_address,
            us.contract_address AS exchange_contract_address,
            call_tx_hash,
            call_trace_address AS trace_address,
            NULL::integer AS evt_index
        FROM (
            select "output_returnAmount", "amount", "srcToken", "_3" as pools, "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" 
            from oneinch_v3."AggregationRouterV3_call_unoswap" 
            where call_success 
                and call_block_time >= start_ts
                and call_block_time < end_ts
            union all
            select "output_returnAmount", "amount", "srcToken", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" 
            from oneinch_v3."AggregationRouterV3_call_unoswapWithPermit" 
            where call_success
                and call_block_time >= start_ts
                and call_block_time < end_ts
            UNION ALL
            select "output_returnAmount", "amount", "srcToken", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" 
            from oneinch_v4."AggregationRouterV4_call_unoswap"  
            where call_success 
                and call_block_time >= start_ts
                and call_block_time < end_ts
            union all
            select "output_returnAmount", "amount", "srcToken", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" 
            from oneinch_v4."AggregationRouterV4_call_unoswapWithPermit"  
            where call_success
                and call_block_time >= start_ts
                and call_block_time < end_ts
        ) us
        left join ethereum.traces t on us.call_tx_hash = t.tx_hash and us.call_trace_address = t.trace_address
            and t.block_time >= start_ts
            and t.block_time < end_ts
        LEFT JOIN ethereum.traces tr ON tr.tx_hash = us.call_tx_hash 
            AND tr.trace_address = us.call_trace_address[:ARRAY_LENGTH(us.call_trace_address, 1)-1]
            and tr.block_time >= start_ts
            and tr.block_time < end_ts
        LEFT JOIN ethereum.traces ll ON ll.tx_hash = us.call_tx_hash 
            AND ll.trace_address = (
                us.call_trace_address || (ARRAY_LENGTH("pools", 1)*2 + CASE WHEN "srcToken" = '\x0000000000000000000000000000000000000000' THEN 1 ELSE 0 END) || 0
                )
            and ll.block_time >= start_ts
            and ll.block_time < end_ts

        UNION ALL

        -- 1inch Uniswap V3 Router
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            'UNI v3' AS version,
            'Aggregator' AS category,
            trader_a AS trader_a,
            NULL::bytea AS trader_b,
            "output_returnAmount" AS token_a_amount_raw,
            "amount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            "dstToken" AS token_a_address,
            "srcToken" AS token_b_address,
            us.contract_address AS exchange_contract_address,
            call_tx_hash,
            call_trace_address AS trace_address,
            NULL::integer AS evt_index
        FROM (
            select
                "output_returnAmount"
                , "amount"
                ,COALESCE((
                    select tr1.to 
                    from ethereum.traces tr1 
                    where call_type = 'call' 
                    and tr1.block_time >= start_ts
                    and tr1.block_time < end_ts
                    and tr1.tx_hash = call_tx_hash 
                    and substring(tr1.input from 1 for 4) = '\x23b872dd'
                    and COALESCE(call_trace_address, array[]::int[]) = tr1.trace_address[:COALESCE(ARRAY_LENGTH(call_trace_address, 1), 0)]
                    and COALESCE(ARRAY_LENGTH(call_trace_address, 1), 0) + 3 = COALESCE(ARRAY_LENGTH(tr1.trace_address, 1), 0)
                    order by COALESCE(trace_address, array[]::int[])
                    LIMIT 1
                )
                , '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') as "srcToken",
                CASE WHEN ((pools[array_length(pools, 1)] / 2^252)::int & 2 <> 0) THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                ELSE
                    (
                        select tr2.to
                        from ethereum.traces tr2 
                        where call_type = 'call' 
                        and tr2.block_time >= start_ts
                        and tr2.block_time < end_ts
                        and tr2.tx_hash = call_tx_hash 
                        and substring(tr2.input from 1 for 4) = '\xa9059cbb'
                        and COALESCE(call_trace_address, array[]::int[]) = tr2.trace_address[:COALESCE(ARRAY_LENGTH(call_trace_address, 1), 0)]
                        and COALESCE(ARRAY_LENGTH(call_trace_address, 1), 0) + 2 = COALESCE(ARRAY_LENGTH(tr2.trace_address, 1), 0)
                        and tr2.from <> contract_address
                        order by COALESCE(trace_address, array[]::int[]) desc
                        LIMIT 1
                    )
                END as "dstToken"
                ,"pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address", t."from" as trader_a
            from (
                select "output_returnAmount", "amount", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch_v4."AggregationRouterV4_call_uniswapV3Swap" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
                select "output_returnAmount", "amount", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch_v4."AggregationRouterV4_call_uniswapV3SwapTo" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
                select "output_returnAmount", "amount", "pools", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch_v4."AggregationRouterV4_call_uniswapV3SwapToWithPermit" where call_success and call_block_time >= start_ts and call_block_time < end_ts
            ) sw
            left join ethereum.traces t 
            on t.tx_hash = sw.call_tx_hash 
            and t.trace_address = sw.call_trace_address
            and t.block_time >= start_ts
            and t.block_time < end_ts
        ) us

        UNION ALL

        -- 1inch Clipper Router
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            'CLIPPER v1' AS version,
            'Aggregator' AS category,
            t."from" AS trader_a,
            NULL::bytea AS trader_b,
            "output_returnAmount" AS token_a_amount_raw,
            "amount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            (CASE WHEN "dstToken" = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE "dstToken" END) AS token_a_address,
            (CASE WHEN "srcToken" = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE "srcToken" END) AS token_b_address,
            us.contract_address AS exchange_contract_address,
            call_tx_hash,
            call_trace_address AS trace_address,
            NULL::integer AS evt_index
        FROM (
            select "output_returnAmount", "amount", "srcToken", "dstToken", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch_v4."AggregationRouterV4_call_clipperSwap" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select "output_returnAmount", "amount", "srcToken", "dstToken", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch_v4."AggregationRouterV4_call_clipperSwapTo" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select "output_returnAmount", "amount", "srcToken", "dstToken", "call_tx_hash", "call_trace_address", "call_block_time", "contract_address" from oneinch_v4."AggregationRouterV4_call_clipperSwapToWithPermit" where call_success and call_block_time >= start_ts and call_block_time < end_ts
        ) us
        LEFT JOIN ethereum.traces t on t.tx_hash = us.call_tx_hash and t.trace_address = us.call_trace_address
            and t.block_time >= start_ts
            and t.block_time < end_ts

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
            select '1' as version, decode(substring("order"::jsonb->>'makerAssetData' from 35 for 40), 'hex') as maker, contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch_lop."LimitOrderProtocol_call_fillOrder" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select '2' as version, decode(substring("order"::jsonb->>'maker' from 3 for 40), 'hex') as maker, contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch_lop_v2."LimitOrderProtocol_call_fillOrder" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select '2' as version, decode(substring("order"::jsonb->>'maker' from 3 for 40), 'hex') as maker, contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch_lop_v2."LimitOrderProtocol_call_fillOrderTo" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select '2' as version, decode(substring("order"::jsonb->>'maker' from 3 for 40), 'hex') as maker, contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch_lop_v2."LimitOrderProtocol_call_fillOrderToWithPermit" where call_success and call_block_time >= start_ts and call_block_time < end_ts
        ) call
        LEFT JOIN ethereum.traces ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            and ts.block_time >= start_ts
            and ts.block_time < end_ts

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
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch_v4."AggregationRouterV4_call_fillOrderRFQ" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch_v4."AggregationRouterV4_call_fillOrderRFQTo" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch_v4."AggregationRouterV4_call_fillOrderRFQToWithPermit" where call_success and call_block_time >= start_ts and call_block_time < end_ts
        ) tt
        LEFT JOIN ethereum.traces ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            and ts.block_time >= start_ts
            and ts.block_time < end_ts

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
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch_v4."AggregationRouterV4_call_fillOrderRFQ" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch_v4."AggregationRouterV4_call_fillOrderRFQTo" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select "call_block_time", "order", "output_0", "output_1", "contract_address", "call_tx_hash", "call_trace_address" from oneinch_v4."AggregationRouterV4_call_fillOrderRFQToWithPermit" where call_success and call_block_time >= start_ts and call_block_time < end_ts
        ) tt
        LEFT JOIN ethereum.traces ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            and ts.block_time >= start_ts
            and ts.block_time < end_ts

        UNION ALL

        -- 1inch Limit Order Protocol RFQ v1
        SELECT
            call_block_time as block_time,
            '1inch Limit Order Protocol' AS project,
            'RFQ v1' AS version,
            'DEX' AS category,
            ts."from"  AS trader_a,
            decode(substring("order"::jsonb->>'makerAssetData' from 35 for 40), 'hex') AS trader_b,
            bytea2numeric(substring(tf2.input from 69 for 32)) AS token_a_amount_raw,
            bytea2numeric(substring(tf1.input from 69 for 32)) AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            decode(substring("order"::jsonb->>'takerAsset' from 3), 'hex') AS token_a_address,
            decode(substring("order"::jsonb->>'makerAsset' from 3), 'hex') AS token_b_address,
            contract_address AS exchange_contract_address,
            call_tx_hash,
            call_trace_address,
            NULL AS evt_index
        FROM oneinch."LimitOrderProtocol_call_fillOrderRFQ" call
        LEFT JOIN ethereum.traces ts 
            ON call_tx_hash = ts.tx_hash 
            AND ts.trace_address = call_trace_address
            and ts.block_time >= start_ts
            and ts.block_time < end_ts
        LEFT JOIN ethereum.traces tf1 
            ON call_tx_hash = tf1.tx_hash 
            AND tf1.trace_address = COALESCE(call_trace_address, '{}') || (ts.sub_traces-2)
            and tf1.block_time >= start_ts
            and tf1.block_time < end_ts
        LEFT JOIN ethereum.traces tf2 
            ON call_tx_hash = tf2.tx_hash 
            AND tf2.trace_address = COALESCE(call_trace_address, '{}') || (ts.sub_traces-1)
            and tf2.block_time >= start_ts
            and tf2.block_time < end_ts
        WHERE call.call_success 
            and call.call_block_time >= start_ts 
            and call.call_block_time < end_ts

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
            select contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch_lop_v2."LimitOrderProtocol_call_fillOrderRFQ" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch_lop_v2."LimitOrderProtocol_call_fillOrderRFQTo" where call_success and call_block_time >= start_ts and call_block_time < end_ts union all
            select contract_address, "order", output_0, output_1, call_block_time, call_tx_hash, call_trace_address from oneinch_lop_v2."LimitOrderProtocol_call_fillOrderRFQToWithPermit" where call_success and call_block_time >= start_ts and call_block_time < end_ts
        ) call
        LEFT JOIN ethereum.traces ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            and ts.block_time >= start_ts
            and ts.block_time < end_ts
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
    LEFT JOIN prices.layer1_usd pe ON pe.minute = date_trunc('minute', dexs.block_time)
        AND pe.symbol = 'ETH'
        AND pe.minute >= start_ts
        AND pe.minute < end_ts

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- --delete prior to reload of history (commented out to be safe, uncomment as needed)
-- delete from
--   dex.trades
-- where
--   project in ('1inch', '1inch Limit Order Protocol')
-- ;

-- fill 2017
SELECT dex.insert_1inch(
    '2017-01-01',
    '2017-07-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2017-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2017-07-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2017-01-01'
--     AND block_time <= '2018-01-01'
--     AND project = '1inch'
-- )
;

-- fill 2017
SELECT dex.insert_1inch(
    '2017-07-01',
    '2018-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2017-07-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2018-01-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2017-01-01'
--     AND block_time <= '2018-01-01'
--     AND project = '1inch'
-- )
;

-- fill 2018
SELECT dex.insert_1inch(
    '2018-01-01',
    '2018-07-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2018-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2018-07-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2018-01-01'
--     AND block_time <= '2019-01-01'
--     AND project = '1inch'
-- )
;

-- fill 2018
SELECT dex.insert_1inch(
    '2018-07-01',
    '2019-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2018-07-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2019-01-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2018-01-01'
--     AND block_time <= '2019-01-01'
--     AND project = '1inch'
-- )
;

-- fill 2019
SELECT dex.insert_1inch(
    '2019-01-01',
    '2019-07-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2019-07-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2019-01-01'
--     AND block_time <= '2020-01-01'
--     AND project = '1inch'
-- )
;

-- fill 2019
SELECT dex.insert_1inch(
    '2019-07-01',
    '2020-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-07-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-01-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2019-01-01'
--     AND block_time <= '2020-01-01'
--     AND project = '1inch'
-- )
;

-- fill 2020
SELECT dex.insert_1inch(
    '2020-01-01',
    '2020-07-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-07-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2020-01-01'
--     AND block_time <= '2021-01-01'
--     AND project = '1inch'
-- )
;

-- fill 2020
SELECT dex.insert_1inch(
    '2020-07-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-07-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2020-01-01'
--     AND block_time <= '2021-01-01'
--     AND project = '1inch'
-- )
;

-- fill 2021
SELECT dex.insert_1inch(
    '2021-01-01',
    '2021-07-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time <= '2021-07-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2021-01-01'
--     AND block_time <= now() - interval '20 minutes'
--     AND project = '1inch'
-- )
;

-- fill 2021
SELECT dex.insert_1inch(
    '2021-07-01',
    '2022-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-07-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time <= '2022-01-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2021-01-01'
--     AND block_time <= now() - interval '20 minutes'
--     AND project = '1inch'
-- )
;

-- fill 2022
SELECT dex.insert_1inch(
    '2022-01-01',
    '2022-07-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2022-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time <= '2022-07-01')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2021-01-01'
--     AND block_time <= now() - interval '20 minutes'
--     AND project = '1inch'
-- )
;

-- fill 2022
SELECT dex.insert_1inch(
    '2022-07-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2022-07-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
-- not applicable as data is removed prior to reload
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM dex.trades
--     WHERE block_time > '2021-01-01'
--     AND block_time <= now() - interval '20 minutes'
--     AND project = '1inch'
-- )
;

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_1inch(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='1inch'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='1inch')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
