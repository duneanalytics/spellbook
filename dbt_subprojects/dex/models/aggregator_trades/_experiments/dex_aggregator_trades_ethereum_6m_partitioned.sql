{{ config(
        schema = 'dex_aggregator'
        , alias = 'trades_ethereum_6m_partitioned'
        , materialized = 'table'
        , file_format = 'delta'
        , partition_by = ['block_month']
    )
}}

SELECT
    blockchain
    , project
    , version
    , block_date
    , block_month
    , block_time
    , token_bought_symbol
    , token_sold_symbol
    , token_pair
    , token_bought_amount
    , token_sold_amount
    , token_bought_amount_raw
    , token_sold_amount_raw
    , amount_usd
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , trace_address
    , evt_index
    , _updated_at
FROM {{ ref('dex_aggregator_trades') }}
WHERE blockchain = 'ethereum'
    AND block_month >= TIMESTAMP '2026-01-01 00:00:00'
    AND block_month < TIMESTAMP '2026-07-01 00:00:00'
