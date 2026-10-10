{{ config(
    schema = 'sushiswap_v3_stellar'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Structured SushiSwap V3 Stellar trades from swap_evt.
-- Twin source rows are already dropped there via operation_id IS NOT NULL.

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'block_date'
        , 'to_hex(tx_hash)'
        , 'transaction_id'
        , 'contract_id'
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
    , CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw
    , CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
    , CASE WHEN amount0 < INT256 '0' THEN token0 ELSE token1 END AS token_bought_address
    , CASE WHEN amount0 < INT256 '0' THEN token1 ELSE token0 END AS token_sold_address
    , recipient AS taker
    , pool AS maker
    , pool AS project_contract_address
    , sender
    , recipient
    , amount0
    , amount1
    , liquidity
    , sqrt_price_x96
    , tick
    , fee
    , tick_spacing
    , token0
    , token1
    , tx_hash
    , transaction_id
    , operation_id
FROM {{ ref('sushiswap_v3_stellar_swap_evt') }}
WHERE event_name = 'swap'
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_time') }}
    {% endif -%}
