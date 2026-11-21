{{ config(
    alias = 'trades',
    schema = 'squeeze',
    materialized = 'view'
) }}

-- EVM attribution trades only (aligned columns). Solana stays in squeeze_solana.trades.

SELECT
    block_time
    , block_date
    , block_month
    , blockchain
    , project
    , dex_project
    , dex_version
    , token_bought_amount
    , token_sold_amount
    , token_bought_symbol
    , token_sold_symbol
    , token_bought_address
    , token_sold_address
    , amount_usd
    , user
    , tx_hash
    , evt_index
FROM {{ ref('squeeze_base_trades') }}

UNION ALL

SELECT
    block_time
    , block_date
    , block_month
    , blockchain
    , project
    , dex_project
    , dex_version
    , token_bought_amount
    , token_sold_amount
    , token_bought_symbol
    , token_sold_symbol
    , token_bought_address
    , token_sold_address
    , amount_usd
    , user
    , tx_hash
    , evt_index
FROM {{ ref('squeeze_robinhood_trades') }}
