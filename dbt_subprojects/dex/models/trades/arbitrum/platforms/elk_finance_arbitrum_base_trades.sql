{{ config(
    schema = 'elk_finance_arbitrum',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
) }}

WITH token_swaps AS (
    SELECT
        s.evt_block_number AS block_number,
        CAST(s.evt_block_time AS timestamp(3) WITH time zone) AS block_time,
        s.evt_tx_from AS maker,
        s.to AS taker,

        -- Raw amounts
        s.amount0In,
        s.amount0Out,
        s.amount1In,
        s.amount1Out,

        -- Determine sold and bought amounts based on input/output values
        CASE 
            WHEN s.amount0In > 0 THEN s.amount0In 
            ELSE s.amount1In 
        END AS token_sold_amount_raw,

        CASE 
            WHEN s.amount0Out > 0 THEN s.amount0Out 
            ELSE s.amount1Out 
        END AS token_bought_amount_raw,

        -- We'll assign token addresses after the join
        f.token0,
        f.token1,

        CASE 
            WHEN s.amount0In > 0 THEN f.token0
            ELSE f.token1
        END AS token_sold_address,

        CASE 
            WHEN s.amount0Out > 0 THEN f.token0
            ELSE f.token1
        END AS token_bought_address,

        CAST(s.contract_address AS varbinary) AS project_contract_address,
        s.evt_tx_hash AS tx_hash,
        s.evt_index AS evt_index
    FROM 
        {{ source('elk_finance', 'ElkPair_evt_Swap') }} s
    LEFT JOIN 
        {{ source('elk_finance', 'ElkFactory_evt_PairCreated') }} f
        ON s.contract_address = f.pair
    {% if is_incremental() %}
    WHERE 
        {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
)

SELECT
    'arbitrum' AS blockchain,
    'elk_finance' AS project,
    '1' AS version,
    CAST(date_trunc('month', token_swaps.block_time) AS date) AS block_month,
    CAST(date_trunc('day', token_swaps.block_time) AS date) AS block_date,
    token_swaps.block_time,
    token_swaps.block_number,
    token_swaps.token_sold_amount_raw,
    token_swaps.token_bought_amount_raw,
    token_swaps.token_sold_address,
    token_swaps.token_bought_address,
    token_swaps.maker,
    token_swaps.taker,
    token_swaps.project_contract_address,
    token_swaps.tx_hash,
    token_swaps.evt_index
FROM 
    token_swaps
