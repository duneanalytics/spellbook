{{ config(
    schema = 'sushiswap_v3_stellar'
    , alias = 'base_liquidity_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Structured SushiSwap V3 Stellar liquidity events from liquidity_evt.
-- Twin source rows are already dropped there via operation_id IS NOT NULL.
-- Mint amounts stay positive (tokens into the pool); burn and collect are signed negative.

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'block_date'
        , 'to_hex(tx_hash)'
        , 'transaction_id'
        , 'contract_id'
        , 'event_name'
        , 'topics'
        , 'data'
    ]) }} AS surrogate_key
    , blockchain
    , project
    , version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , block_date
    , block_time
    , ledger_sequence AS block_number
    , event_name AS event_type
    , pool
    , pool AS id
    , token0
    , token1
    , fee
    , tick_spacing
    , CASE
        WHEN event_name = 'burn' THEN -1 * CAST(amount AS int256)
        ELSE CAST(amount AS int256)
    END AS liquidity_raw
    , CASE
        WHEN event_name IN ('burn', 'collect') THEN -1 * abs(amount0)
        ELSE abs(amount0)
    END AS amount0_raw
    , CASE
        WHEN event_name IN ('burn', 'collect') THEN -1 * abs(amount1)
        ELSE abs(amount1)
    END AS amount1_raw
    , tick_lower
    , tick_upper
    , sender
    , owner
    , recipient
    , tx_hash
    , transaction_id
    , operation_id
FROM {{ ref('sushiswap_v3_stellar_liquidity_evt') }}
WHERE event_name IN ('mint', 'burn', 'collect')
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_time') }}
    {% endif -%}
