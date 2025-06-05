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
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) WITH time zone) AS block_time,
        evt_tx_from AS maker,
        to AS taker,
        -- Calculate token amounts
        CASE 
            WHEN amount0In > 0 THEN amount0In 
            ELSE amount1In 
        END AS token_sold_amount_raw,
        
        CASE 
            WHEN amount0Out > 0 THEN amount0Out 
            ELSE amount1Out 
        END AS token_bought_amount_raw,

        -- We assume token0 is sold and token1 is bought or vice versa
        NULL AS token_sold_address,  -- Not available from event data
        NULL AS token_bought_address,  -- Not available from event data
        
        CAST(contract_address AS varbinary) AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM 
        {{ source('elk_finance_arbitrum', 'ElkPair_evt_Swap') }}
    {% if is_incremental() %}
    WHERE 
        {{ incremental_predicate('evt_block_time') }}
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
