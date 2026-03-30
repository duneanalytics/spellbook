{{
    config(
        schema = 'tempo_exchange_tempo',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH swap_exact_in AS (
    SELECT
        call_block_time AS block_time
        , call_block_number AS block_number
        , tokenIn AS token_sold_address
        , tokenOut AS token_bought_address
        , CAST(amountIn AS uint256) AS token_sold_amount_raw
        , CAST(output_amountOut AS uint256) AS token_bought_amount_raw
        , contract_address AS project_contract_address
        , call_tx_hash AS tx_hash
        , ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_block_time) AS row_num
    FROM {{ source('tempoexchange_tempo', 'stablecoindex_call_swapexactamountin') }}
    WHERE call_success = true
    {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_time') }}
    {% endif -%}
)

, swap_exact_out AS (
    SELECT
        call_block_time AS block_time
        , call_block_number AS block_number
        , tokenIn AS token_sold_address
        , tokenOut AS token_bought_address
        , CAST(output_amountIn AS uint256) AS token_sold_amount_raw
        , CAST(amountOut AS uint256) AS token_bought_amount_raw
        , contract_address AS project_contract_address
        , call_tx_hash AS tx_hash
        , ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_block_time) AS row_num
    FROM {{ source('tempoexchange_tempo', 'stablecoindex_call_swapexactamountout') }}
    WHERE call_success = true
    {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_time') }}
    {% endif -%}
)

, dexs AS (
    SELECT * FROM swap_exact_in
    UNION ALL
    SELECT * FROM swap_exact_out
)

SELECT
    'tempo' AS blockchain
    , 'tempo_exchange' AS project
    , '1' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , CAST(NULL AS VARBINARY) AS taker
    , CAST(NULL AS VARBINARY) AS maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.row_num AS evt_index
FROM dexs
