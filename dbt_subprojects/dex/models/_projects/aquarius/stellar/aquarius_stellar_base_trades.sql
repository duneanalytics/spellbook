{{ config(
    schema = 'aquarius_stellar'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'surrogate_key']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Structured Aquarius Stellar trades from pool trade_evt.
-- Twin source rows are already dropped there via operation_id IS NOT NULL.
-- Not wired into dex.trades.

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
    , abs(token_bought_amount_raw) AS token_bought_amount_raw
    , abs(token_sold_amount_raw) AS token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , trader AS taker
    , pool AS maker
    , pool AS project_contract_address
    , trader
    , pool
    , pool_type
    , fee_raw
    , token0
    , token1
    , token2
    , tx_hash
    , transaction_id
    , operation_id
FROM {{ ref('aquarius_stellar_trade_evt') }}
WHERE event_name = 'trade'
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_time') }}
    {% endif -%}
