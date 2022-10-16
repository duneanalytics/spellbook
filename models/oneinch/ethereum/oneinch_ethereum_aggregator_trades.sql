{{  config(
        alias='trades',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "1inch",
                                    \'["k06a", "dsalv"]\') }}'
    )
}}

-- {% set project_start_date = '2019-06-03 00:00:00' %}
{% set project_start_date = '2022-09-03 00:00:00' %}

WITH aggregators AS
(
    SELECT
        block_time,
        project,
        version,
        category,
        taker,-- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
        maker,
        token_bought_amount_raw,
        token_sold_amount_raw,
        amount_usd,
        token_bought_address,
        token_sold_address,
        project_contract_address,
        tx_hash,
        trace_address,
        evt_index
    FROM (
        SELECT
            oi.block_time,
            '1inch' AS project,
            version,
            'Aggregator' AS category,
            taker,
            '' AS maker,
            to_amount AS token_bought_amount_raw,
            from_amount AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd,
            (CASE WHEN to_token = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE to_token END) AS token_bought_address,
            (CASE WHEN from_token = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE from_token END) AS token_sold_address,
            contract_address AS project_contract_address,
            tx_hash,
            trace_address,
            evt_index
        FROM
        (
            (
            SELECT t.from as taker, calls.*
                FROM (
                    SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' , 'exchange_v1_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                    SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' , 'exchange_v2_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                    SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' , 'exchange_v3_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                    SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' , 'exchange_v4_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                    SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' , 'exchange_v5_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                    SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' , 'exchange_v6_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                    SELECT fromToken as from_token, toToken as to_token, fromTokenAmount as from_amount, minReturnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' , 'exchange_v7_call_swap') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                    SELECT fromToken as from_token, toToken as to_token, fromTokenAmount as from_amount, minReturnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' , 'OneInchExchange_call_swap') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                    SELECT get_json_object(desc,'$.srcToken') as from_token, get_json_object(desc,'$.dstToken') as to_token, output_spentAmount as from_amount, output_returnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address, CAST(NULL as integer) AS evt_index, contract_address, '4' as version FROM {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_swap') }} where call_success and call_block_time >= '{{project_start_date}}'
                ) calls
                LEFT JOIN {{ source('ethereum', 'traces') }} t on calls.tx_hash = t.tx_hash and calls.trace_address = t.trace_address
                and t.block_time >= '{{project_start_date}}')

            UNION ALL
            SELECT sender as taker, srcToken as from_token, dstToken as to_token, spentAmount as from_amount, returnAmount as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, CAST(NULL AS ARRAY<int>) as call_trace_address, evt_index, contract_address, '2' as version 
            FROM {{ source('oneinch_v2_ethereum', 'OneInchExchange_evt_Swapped') }}
            WHERE
                evt_block_time >= '{{project_start_date}}'
            UNION ALL
            SELECT sender as taker, srcToken as from_token, dstToken as to_token, spentAmount as from_amount, returnAmount as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, CAST(NULL AS ARRAY<int>) as call_trace_address, evt_index, contract_address, '3' as version
            FROM {{ source('oneinch_v3_ethereum', 'AggregationRouterV3_evt_Swapped') }}
            WHERE
                evt_block_time >= '{{project_start_date}}'
        ) oi

        UNION ALL

        SELECT
            oi.block_time,
            '1inch' AS project,
            '1split' as version,
            'Aggregator' AS category,
            t.from AS taker,
            '' AS maker,
            to_amount AS token_bought_amount_raw,
            from_amount AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd,
            (CASE WHEN to_token = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE to_token END) AS token_bought_address,
            (CASE WHEN from_token = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE from_token END) AS token_sold_address,
            contract_address AS project_contract_address,
            oi.tx_hash,
            call_trace_address as trace_address,
            CAST(NULL as integer) AS evt_index
        FROM (
            SELECT fromToken AS from_token, toToken AS to_token, amount AS from_amount, "minReturn" AS to_amount, call_tx_hash AS tx_hash, call_trace_address, call_block_time AS block_time, contract_address FROM {{ source('onesplit_ethereum', 'OneSplit_call_swap') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
            SELECT fromToken AS from_token, toToken AS to_token, amount AS from_amount, "minReturn" AS to_amount, call_tx_hash AS tx_hash, call_trace_address, call_block_time AS block_time, contract_address FROM {{ source('onesplit_ethereum', 'OneSplit_call_goodSwap') }} WHERE call_success and call_block_time >= '{{project_start_date}}'
        ) oi
        left join {{ source('ethereum', 'traces') }} t on oi.tx_hash = t.tx_hash and oi.call_trace_address = t.trace_address
            and t.block_time >= '{{project_start_date}}'
        where oi.tx_hash not in (
            select tx_hash from (
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum', 'exchange_v1_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum', 'exchange_v2_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum', 'exchange_v3_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum', 'exchange_v4_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum', 'exchange_v5_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum', 'exchange_v6_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, fromTokenAmount as from_amount, minReturnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum', 'exchange_v7_call_swap') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, fromTokenAmount as from_amount, minReturnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum', 'OneInchExchange_call_swap') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT srcToken as from_token, dstToken as to_token, spentAmount as from_amount, returnAmount as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, CAST(NULL AS ARRAY<int>) as call_trace_address, evt_index, contract_address, '2' as version 
                    FROM {{ source('oneinch_v2_ethereum', 'OneInchExchange_evt_Swapped') }}
                    WHERE
                        evt_block_time >= '{{project_start_date}}'
                UNION ALL
                SELECT srcToken as from_token, dstToken as to_token, spentAmount as from_amount, returnAmount as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, CAST(NULL AS ARRAY<int>) as call_trace_address, evt_index, contract_address, '3' as version 
                    FROM {{ source('oneinch_v3_ethereum', 'AggregationRouterV3_evt_Swapped') }}
                    WHERE
                        evt_block_time >= '{{project_start_date}}'
                UNION ALL
                SELECT get_json_object(desc,'$.srcToken') as from_token, get_json_object(desc,'$.dstToken') as to_token, output_spentAmount as from_amount, output_returnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address, CAST(NULL as integer) AS evt_index, contract_address, '4' as version FROM {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_swap') }} where call_success and call_block_time >= '{{project_start_date}}'
            ) calls
        )

        UNION ALL

        SELECT
            oi.block_time,
            '1inch' AS project,
            '1proto' as version,
            'Aggregator' AS category,
            tx.from AS taker,
            '' AS maker,
            to_amount AS token_bought_amount_raw,
            from_amount AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd,
            (CASE WHEN to_token = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE to_token END) AS token_bought_address,
            (CASE WHEN from_token = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE from_token END) AS token_sold_address,
            contract_address AS project_contract_address,
            tx_hash,
            CAST(NULL AS ARRAY<int>) as trace_address,
            evt_index
        FROM (
            SELECT fromToken as from_token, "destToken" as to_token, fromTokenAmount as from_amount, "destTokenAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, contract_address, evt_index FROM {{ source('oneproto_ethereum' ,'OneSplitAudit_evt_Swapped') }}
            WHERE
                evt_block_time >= '{{project_start_date}}'
        ) oi
        left join ethereum.transactions tx on hash = tx_hash
            and tx.block_time >= '{{project_start_date}}'
        where tx_hash not in (
            select tx_hash from (
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' ,'exchange_v1_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' ,'exchange_v2_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' ,'exchange_v3_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' ,'exchange_v4_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' ,'exchange_v5_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, tokensAmount as from_amount, minTokensAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' ,'exchange_v6_call_aggregate') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, fromTokenAmount as from_amount, minReturnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' ,'exchange_v7_call_swap') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT fromToken as from_token, toToken as to_token, fromTokenAmount as from_amount, minReturnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, CAST(NULL as integer) AS evt_index, contract_address, '1' as version FROM {{ source('oneinch_ethereum' ,'OneInchExchange_call_swap') }} WHERE call_success and call_block_time >= '{{project_start_date}}' UNION ALL
                SELECT srcToken as from_token, dstToken as to_token, spentAmount as from_amount, returnAmount as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, CAST(NULL AS ARRAY<int>) as call_trace_address, evt_index, contract_address, '2' as version FROM {{ source('oneinch_v2_ethereum', 'OneInchExchange_evt_Swapped') }} where evt_block_time >= '{{project_start_date}}' UNION ALL
                SELECT srcToken as from_token, dstToken as to_token, spentAmount as from_amount, returnAmount as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, CAST(NULL AS ARRAY<int>) as call_trace_address, evt_index, contract_address, '3' as version FROM {{ source('oneinch_v3_ethereum', 'AggregationRouterV3_evt_Swapped') }} where evt_block_time >= '{{project_start_date}}' UNION ALL
                SELECT get_json_object(desc,'$.srcToken') as from_token, get_json_object(desc,'$.dstToken') as to_token, output_spentAmount as from_amount, output_returnAmount as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address, CAST(NULL as integer) AS evt_index, contract_address, '4' as version FROM {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_swap') }} where call_success and call_block_time >= '{{project_start_date}}'
            ) t
        )
        
        UNION ALL

        -- 1inch 0x Limit Orders
        SELECT
            evt_block_time as block_time,
            '1inch' AS project,
            'ZRX' AS version,
            'Aggregator' AS category,
            takerAddress AS taker,
            makerAddress AS maker,
            takerAssetFilledAmount AS token_bought_amount_raw,
            makerAssetFilledAmount AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd,
            CONCAT("0x", substring(takerAssetData, 35, 40)) AS token_bought_address,
            CONCAT("0x", substring(makerAssetData, 35, 40)) AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash,
            CAST(NULL AS ARRAY<int>) AS trace_address,
            evt_index
        FROM (
            select feeRecipientAddress, takerAssetData, makerAssetData, makerAddress, takerAddress, makerAssetFilledAmount, takerAssetFilledAmount, contract_address, evt_block_time, evt_tx_hash, evt_index
            from {{ source('zeroex_v2_ethereum', 'Exchange2_0_evt_Fill') }}
            where evt_block_time >= '{{project_start_date}}'
            union all -- 0x v1
            select feeRecipientAddress, takerAssetData, makerAssetData, makerAddress, takerAddress, makerAssetFilledAmount, takerAssetFilledAmount, contract_address, evt_block_time, evt_tx_hash, evt_index 
            from {{ source('zeroex_v2_ethereum', 'Exchange2_1_evt_Fill') }} 
            where evt_block_time >= '{{project_start_date}}'
            union all -- 0x v2
            select feeRecipientAddress, takerAssetData, makerAssetData, makerAddress, takerAddress, makerAssetFilledAmount, takerAssetFilledAmount, contract_address, evt_block_time, evt_tx_hash, evt_index 
            from {{ source('zeroex_v3_ethereum', 'Exchange_evt_Fill') }}
            where evt_block_time >= '{{project_start_date}}'
            union all -- 0x v3
            select feeRecipient, takerToken, makerToken, maker, taker, makerTokenFilledAmount, takerTokenFilledAmount, contract_address, evt_block_time, evt_tx_hash, evt_index 
            from {{ source('zeroex_ethereum', 'ExchangeProxy_evt_LimitOrderFilled') }} -- 0x v4
            where evt_block_time >= '{{project_start_date}}'
        ) oi
        WHERE feeRecipientAddress IN ('0x910bf2d50fa5e014fd06666f456182d4ab7c8bd2', '0x68a17b587caf4f9329f0e372e3a78d23a46de6b5')

        UNION ALL

        -- 1inch Unoswap
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            'UNI v2' AS version,
            'Aggregator' AS category,
            t.from AS taker,
            '' AS maker,
            output_returnAmount AS token_bought_amount_raw,
            amount AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd,
            (CASE WHEN ll.to = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND substring(pools[size(pools)-1], 1, 4) IN ('0xc0', '0x40') THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE ll.to END) AS token_bought_address,
            (CASE WHEN srcToken = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE srcToken END) AS token_sold_address,
            us.contract_address AS project_contract_address,
            call_tx_hash,
            call_trace_address AS trace_address,
            CAST(NULL as integer) AS evt_index
        FROM (
            -- select output_returnAmount, amount, srcToken, "_3" as pools, call_tx_hash, call_trace_address, call_block_time, contract_address 
            -- from {{ source('oneinch_v3_ethereum', 'AggregationRouterV3_call_unoswap') }}
            -- where call_success 
            --     and call_block_time >= '{{project_start_date}}'
               
            -- union all
            select output_returnAmount, amount, srcToken, pools, call_tx_hash, call_trace_address, call_block_time, contract_address 
            from {{ source('oneinch_v3_ethereum', 'AggregationRouterV3_call_unoswapWithPermit') }}
            where call_success
                and call_block_time >= '{{project_start_date}}'
               
            UNION ALL
            select output_returnAmount, amount, srcToken, pools, call_tx_hash, call_trace_address, call_block_time, contract_address 
            from {{ source('oneinch_v4_ethereum' , 'AggregationRouterV4_call_unoswap') }}  
            where call_success 
                and call_block_time >= '{{project_start_date}}'
               
            union all
            select output_returnAmount, amount, srcToken, pools, call_tx_hash, call_trace_address, call_block_time, contract_address 
            from {{ source('oneinch_v4_ethereum' , 'AggregationRouterV4_call_unoswapWithPermit') }}
            where call_success
                and call_block_time >= '{{project_start_date}}'
               
        ) us
        left join {{ source('ethereum', 'traces') }} t on us.call_tx_hash = t.tx_hash and us.call_trace_address = t.trace_address
            and t.block_time >= '{{project_start_date}}'
        LEFT JOIN {{ source('ethereum', 'traces') }} tr ON tr.tx_hash = us.call_tx_hash 
            AND tr.trace_address = us.call_trace_address[0:SIZE(us.call_trace_address)-1]
            and tr.block_time >= '{{project_start_date}}'
        LEFT JOIN {{ source('ethereum', 'traces') }} ll ON ll.tx_hash = us.call_tx_hash 
            -- AND ll.trace_address = (
            --     us.call_trace_address || (SIZE(pools, 1)*2 + CASE WHEN srcToken = '0x0000000000000000000000000000000000000000' THEN 1 ELSE 0 END) || 0
            --     )
            and ll.block_time >= '{{project_start_date}}'

        -- UNION ALL

        -- -- 1inch Uniswap V3 Router
        -- SELECT
        --     call_block_time as block_time,
        --     '1inch' AS project,
        --     'UNI v3' AS version,
        --     'Aggregator' AS category,
        --     taker AS taker,
        --     '' AS maker,
        --     output_returnAmount AS token_bought_amount_raw,
        --     amount AS token_sold_amount_raw,
        --     CAST(NULL as DOUBLE) AS amount_usd,
        --     dstToken AS token_bought_address,
        --     srcToken AS token_sold_address,
        --     us.contract_address AS project_contract_address,
        --     call_tx_hash,
        --     call_trace_address AS trace_address,
        --     CAST(NULL as integer) AS evt_index
        -- FROM (
        --     select
        --         output_returnAmount
        --         , amount
        --         ,COALESCE((
        --             select tr1.to 
        --             from {{ source('ethereum', 'traces') }} tr1 
        --             where call_type = 'call' 
        --             and tr1.block_time >= '{{project_start_date}}'
        --             and tr1.tx_hash = call_tx_hash 
        --             and substring(tr1.input, 1, 10) = '0x23b872dd'
        --             -- and COALESCE(call_trace_address, CAST(NULL AS ARRAY<int>))) = tr1.trace_address[:COALESCE(SIZE(call_trace_address), 0)]
        --             and COALESCE(SIZE(call_trace_address), 0) + 3 = COALESCE(SIZE(tr1.trace_address), 0)
        --             order by COALESCE(trace_address, CAST(NULL AS ARRAY<int>)))
        --             -- LIMIT 1
        --         )
        --         , '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') as srcToken,
        --         CASE WHEN ((pools[SIZE(pools)] / 2^252)::int & 2 <> 0) THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        --         ELSE
        --             (
        --                 select tr2.to
        --                 from {{ source('ethereum', 'traces') }} tr2 
        --                 where call_type = 'call' 
        --                 and tr2.block_time >= '{{project_start_date}}'
        --                 and tr2.tx_hash = call_tx_hash 
        --                 and substring(tr2.input, 1, 10) = '0xa9059cbb'
        --                 and COALESCE(call_trace_address, CAST(NULL AS ARRAY<int>))) = tr2.trace_address[:COALESCE(SIZE(call_trace_address), 0)]
        --                 and COALESCE(SIZE(call_trace_address), 0) + 2 = COALESCE(SIZE(tr2.trace_address), 0)
        --                 and tr2.from <> contract_address
        --                 order by COALESCE(trace_address, CAST(NULL AS ARRAY<int>))) desc
        --                 -- LIMIT 1
        --             )
        --         END as dstToken
        --         ,pools, call_tx_hash, call_trace_address, call_block_time, contract_address, t.from as taker
        --     from (
        --         select output_returnAmount, amount, pools, call_tx_hash, call_trace_address, call_block_time, contract_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_uniswapV3Swap') }} where call_success and call_block_time >= '{{project_start_date}}' union all
        --         select output_returnAmount, amount, pools, call_tx_hash, call_trace_address, call_block_time, contract_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_uniswapV3SwapTo') }} where call_success and call_block_time >= '{{project_start_date}}' union all
        --         select output_returnAmount, amount, pools, call_tx_hash, call_trace_address, call_block_time, contract_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_uniswapV3SwapToWithPermit') }} where call_success and call_block_time >= '{{project_start_date}}'
        --     ) sw
        --     left join {{ source('ethereum', 'traces') }} t 
        --     on t.tx_hash = sw.call_tx_hash 
        --     and t.trace_address = sw.call_trace_address
        --     and t.block_time >= '{{project_start_date}}'
        -- ) us

        UNION ALL

        -- 1inch Clipper Router
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            'CLIPPER v1' AS version,
            'Aggregator' AS category,
            t.from AS taker,
            '' AS maker,
            output_returnAmount AS token_bought_amount_raw,
            amount AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd,
            (CASE WHEN dstToken = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE dstToken END) AS token_bought_address,
            (CASE WHEN srcToken = '0x0000000000000000000000000000000000000000' THEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE srcToken END) AS token_sold_address,
            us.contract_address AS project_contract_address,
            call_tx_hash,
            call_trace_address AS trace_address,
            CAST(NULL as integer) AS evt_index
        FROM (
            select output_returnAmount, amount, srcToken, dstToken, call_tx_hash, call_trace_address, call_block_time, contract_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_clipperSwap') }} where call_success and call_block_time >= '{{project_start_date}}' union all
            select output_returnAmount, amount, srcToken, dstToken, call_tx_hash, call_trace_address, call_block_time, contract_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_clipperSwapTo') }} where call_success and call_block_time >= '{{project_start_date}}' union all
            select output_returnAmount, amount, srcToken, dstToken, call_tx_hash, call_trace_address, call_block_time, contract_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_clipperSwapToWithPermit') }} where call_success and call_block_time >= '{{project_start_date}}'
        ) us
        LEFT JOIN {{ source('ethereum', 'traces') }} t on t.tx_hash = us.call_tx_hash and t.trace_address = us.call_trace_address
            and t.block_time >= '{{project_start_date}}'

        UNION ALL

        -- 1inch Embedded RFQ v1
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            'eRFQ v1' AS version,
            'Aggregator' AS category,
            `from`  AS taker,
            get_json_object(order,'$.maker') AS maker,
            output_1 AS token_bought_amount_raw,
            output_0 AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd, 
            get_json_object(order,'$.takerAsset') AS token_bought_address,
            get_json_object(order,'$.makerAsset') AS token_sold_address,
            contract_address AS project_contract_address,
            call_tx_hash,
            trace_address,
            CAST(NULL as integer) AS evt_index
        FROM (
            select call_block_time, order, output_0, output_1, contract_address, call_tx_hash, call_trace_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_fillOrderRFQ') }} where call_success and call_block_time >= '{{project_start_date}}' union all
            select call_block_time, order, output_0, output_1, contract_address, call_tx_hash, call_trace_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_fillOrderRFQTo') }} where call_success and call_block_time >= '{{project_start_date}}' union all
            select call_block_time, order, output_0, output_1, contract_address, call_tx_hash, call_trace_address from {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_fillOrderRFQToWithPermit') }} where call_success and call_block_time >= '{{project_start_date}}'
        ) tt
        LEFT JOIN {{ source('ethereum', 'traces') }} ts ON call_tx_hash = ts.tx_hash AND call_trace_address = ts.trace_address
            and ts.block_time >= '{{project_start_date}}'
    
    )
)


SELECT
    'ethereum' AS blockchain,
    '1inch' AS project,
    aggregators.version AS version,
    TRY_CAST(date_trunc('DAY', aggregators.block_time) AS date) AS block_date,
    aggregators.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair,
    aggregators.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    aggregators.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    aggregators.token_bought_amount_raw,
    aggregators.token_sold_amount_raw,
    coalesce(
        aggregators.amount_usd
        ,(aggregators.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(aggregators.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    aggregators.token_bought_address,
    aggregators.token_sold_address,
    coalesce(aggregators.taker, tx.from) AS taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    aggregators.maker,
    aggregators.project_contract_address,
    aggregators.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    aggregators.trace_address,
    aggregators.evt_index
FROM aggregators
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON tx.hash = aggregators.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time = date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20a ON erc20a.contract_address = aggregators.token_bought_address
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20b ON erc20b.contract_address = aggregators.token_sold_address
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', aggregators.block_time)
    AND p_bought.contract_address = aggregators.token_bought_address
    AND p_bought.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', aggregators.block_time)
    AND p_sold.contract_address = aggregators.token_sold_address
    AND p_sold.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}