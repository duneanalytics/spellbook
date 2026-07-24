{{
    config(
        schema = 'origin_arm_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set deployment_date = "DATE '2023-12-13'" %}

WITH swap_exact_in AS (
    SELECT
        call_block_time AS block_time
        , call_block_number AS block_number
        , call_block_date AS block_date
        , call_trace_address AS trace_address
        , 'exact_in' AS trade_source
        , COALESCE(inToken, ELEMENT_AT(path, 1)) AS token_sold_address
        , COALESCE(outToken, ELEMENT_AT(path, CARDINALITY(path))) AS token_bought_address
        , amountIn AS token_sold_amount_raw
        , ELEMENT_AT(output_amounts, CARDINALITY(output_amounts)) AS token_bought_amount_raw
        , contract_address AS project_contract_address
        , call_tx_hash AS tx_hash
        , call_tx_from AS taker
    FROM {{ source('origin_protocol_ethereum', 'oswapwethsteth_call_swapexacttokensfortokens') }}
    WHERE call_success
        AND contract_address = 0x85b78aca6deae198fbf201c82daf6ca21942acc6
        AND call_block_date >= {{ deployment_date }}
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif -%}

    UNION ALL

    SELECT call_block_time, call_block_number, call_block_date, call_trace_address, 'exact_in'
        , COALESCE(inToken, ELEMENT_AT(path, 1)), COALESCE(outToken, ELEMENT_AT(path, CARDINALITY(path)))
        , amountIn, ELEMENT_AT(output_amounts, CARDINALITY(output_amounts)), contract_address, call_tx_hash, call_tx_from
    FROM {{ source('origin_protocol_ethereum', 'etherfiarm_call_swapexacttokensfortokens') }}
    WHERE call_success
        AND contract_address = 0xfb0a3cf9b019bfd8827443d131b235b3e0fc58d2
        AND call_block_date >= {{ deployment_date }}
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif -%}

    UNION ALL

    SELECT call_block_time, call_block_number, call_block_date, call_trace_address, 'exact_in'
        , COALESCE(inToken, ELEMENT_AT(path, 1)), COALESCE(outToken, ELEMENT_AT(path, CARDINALITY(path)))
        , amountIn, ELEMENT_AT(output_amounts, CARDINALITY(output_amounts)), contract_address, call_tx_hash, call_tx_from
    FROM {{ source('origin_protocol_ethereum', 'ethenaarm_call_swapexacttokensfortokens') }}
    WHERE call_success
        AND contract_address = 0xceda2d856238aa0d12f6329de20b9115f07c366d
        AND call_block_date >= {{ deployment_date }}
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif -%}
)

, swap_exact_out AS (
    SELECT
        call_block_time AS block_time
        , call_block_number AS block_number
        , call_block_date AS block_date
        , call_trace_address AS trace_address
        , 'exact_out' AS trade_source
        , COALESCE(inToken, ELEMENT_AT(path, 1)) AS token_sold_address
        , COALESCE(outToken, ELEMENT_AT(path, CARDINALITY(path))) AS token_bought_address
        , ELEMENT_AT(output_amounts, 1) AS token_sold_amount_raw
        , amountOut AS token_bought_amount_raw
        , contract_address AS project_contract_address
        , call_tx_hash AS tx_hash
        , call_tx_from AS taker
    FROM {{ source('origin_protocol_ethereum', 'oswapwethsteth_call_swaptokensforexacttokens') }}
    WHERE call_success
        AND contract_address = 0x85b78aca6deae198fbf201c82daf6ca21942acc6
        AND call_block_date >= {{ deployment_date }}
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif -%}

    UNION ALL

    SELECT call_block_time, call_block_number, call_block_date, call_trace_address, 'exact_out'
        , COALESCE(inToken, ELEMENT_AT(path, 1)), COALESCE(outToken, ELEMENT_AT(path, CARDINALITY(path)))
        , ELEMENT_AT(output_amounts, 1), amountOut, contract_address, call_tx_hash, call_tx_from
    FROM {{ source('origin_protocol_ethereum', 'etherfiarm_call_swaptokensforexacttokens') }}
    WHERE call_success
        AND contract_address = 0xfb0a3cf9b019bfd8827443d131b235b3e0fc58d2
        AND call_block_date >= {{ deployment_date }}
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif -%}

    UNION ALL

    SELECT call_block_time, call_block_number, call_block_date, call_trace_address, 'exact_out'
        , COALESCE(inToken, ELEMENT_AT(path, 1)), COALESCE(outToken, ELEMENT_AT(path, CARDINALITY(path)))
        , ELEMENT_AT(output_amounts, 1), amountOut, contract_address, call_tx_hash, call_tx_from
    FROM {{ source('origin_protocol_ethereum', 'ethenaarm_call_swaptokensforexacttokens') }}
    WHERE call_success
        AND contract_address = 0xceda2d856238aa0d12f6329de20b9115f07c366d
        AND call_block_date >= {{ deployment_date }}
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif -%}
)

, decoded_swaps AS (
    SELECT * FROM swap_exact_in
    UNION ALL
    SELECT * FROM swap_exact_out
)

-- Older ARM implementations return no ABI-encoded amounts array. Recover only the
-- missing leg from the exact ERC-20 transfer call nested beneath each swap trace.
, missing_amounts AS (
    SELECT
        swaps.tx_hash
        , swaps.trace_address
        , swaps.trade_source
        , swaps.project_contract_address
        , SUM(
            CASE
                WHEN swaps.trade_source = 'exact_in' AND VARBINARY_SUBSTRING(traces.input, 1, 4) = 0xa9059cbb
                    THEN VARBINARY_TO_UINT256(VARBINARY_SUBSTRING(traces.input, 37, 32))
                WHEN swaps.trade_source = 'exact_out' AND VARBINARY_SUBSTRING(traces.input, 1, 4) = 0x23b872dd
                    THEN VARBINARY_TO_UINT256(VARBINARY_SUBSTRING(traces.input, 69, 32))
            END
        ) AS amount_raw
    FROM decoded_swaps AS swaps
    INNER JOIN {{ source('ethereum', 'traces') }} AS traces
        ON traces.block_number = swaps.block_number
        AND traces.tx_hash = swaps.tx_hash
        AND traces."from" = swaps.project_contract_address
        AND traces."to" = CASE WHEN swaps.trade_source = 'exact_in' THEN swaps.token_bought_address ELSE swaps.token_sold_address END
        AND CARDINALITY(traces.trace_address) = CARDINALITY(swaps.trace_address) + 1
        AND SLICE(traces.trace_address, 1, CARDINALITY(swaps.trace_address)) = swaps.trace_address
        AND (
            (swaps.trade_source = 'exact_in' AND VARBINARY_SUBSTRING(traces.input, 1, 4) = 0xa9059cbb)
            OR (swaps.trade_source = 'exact_out' AND VARBINARY_SUBSTRING(traces.input, 1, 4) = 0x23b872dd)
        )
    WHERE (swaps.token_sold_amount_raw IS NULL OR swaps.token_bought_amount_raw IS NULL)
        AND traces.block_date >= {{ deployment_date }}
        AND traces.success
        {% if is_incremental() -%}
        AND {{ incremental_predicate('traces.block_time') }}
        {% endif -%}
    GROUP BY 1, 2, 3, 4
)

, trades AS (
    SELECT
        swaps.*
        , COALESCE(swaps.token_sold_amount_raw, IF(swaps.trade_source = 'exact_out', missing_amounts.amount_raw)) AS final_sold_amount_raw
        , COALESCE(swaps.token_bought_amount_raw, IF(swaps.trade_source = 'exact_in', missing_amounts.amount_raw)) AS final_bought_amount_raw
    FROM decoded_swaps AS swaps
    LEFT JOIN missing_amounts
        ON missing_amounts.tx_hash = swaps.tx_hash
        AND missing_amounts.trace_address = swaps.trace_address
        AND missing_amounts.trade_source = swaps.trade_source
        AND missing_amounts.project_contract_address = swaps.project_contract_address
)

SELECT
    'ethereum' AS blockchain
    , 'origin_arm' AS project
    , '1' AS version
    , CAST(DATE_TRUNC('month', trades.block_time) AS DATE) AS block_month
    , trades.block_date
    , trades.block_time
    , trades.block_number
    , CAST(trades.final_bought_amount_raw AS UINT256) AS token_bought_amount_raw
    , CAST(trades.final_sold_amount_raw AS UINT256) AS token_sold_amount_raw
    , trades.token_bought_address
    , trades.token_sold_address
    , trades.taker
    , CAST(NULL AS VARBINARY) AS maker
    , trades.project_contract_address
    , trades.tx_hash
    , ROW_NUMBER() OVER (
        PARTITION BY trades.tx_hash
        ORDER BY trades.trace_address, trades.trade_source, trades.project_contract_address
    ) AS evt_index
FROM trades
WHERE trades.final_sold_amount_raw > UINT256 '0'
    AND trades.final_bought_amount_raw > UINT256 '0'
