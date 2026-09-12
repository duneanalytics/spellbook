{{ config(
    schema = 'aquarius_stellar'
    , alias = 'base_liquidity_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Structured Aquarius Stellar liquidity events from liquidity_evt.
-- Twin source rows are already dropped there via operation_id IS NOT NULL.
-- Deposit amounts stay positive; withdraw amounts are signed negative.

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
    , pool_type
    , token0
    , token1
    , token2
    , CASE
        WHEN event_name = 'withdraw_liquidity' THEN -1 * abs(amount0_raw)
        ELSE abs(amount0_raw)
    END AS amount0_raw
    , CASE
        WHEN event_name = 'withdraw_liquidity' THEN -1 * abs(amount1_raw)
        ELSE abs(amount1_raw)
    END AS amount1_raw
    , CASE
        WHEN event_name = 'withdraw_liquidity' THEN -1 * abs(amount2_raw)
        ELSE amount2_raw
    END AS amount2_raw
    , amount3_raw
    , asset0
    , asset1
    , asset2
    , tx_hash
    , transaction_id
    , operation_id
FROM {{ ref('aquarius_stellar_liquidity_evt') }}
WHERE event_name IN (
        'deposit_liquidity'
        , 'withdraw_liquidity'
        , 'position_update'
        , 'claim_fees'
        , 'claim_reward'
    )
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_time') }}
    {% endif -%}
