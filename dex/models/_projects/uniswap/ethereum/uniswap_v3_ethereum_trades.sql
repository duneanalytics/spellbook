{{ config(
        schema = 'uniswap_v3_ethereum',
        alias = 'trades',
        materialized = 'view'
        )
}}


SELECT  
    dex.blockchain
    , dex.project
    , dex.version
    , dex.block_month
    , dex.block_date
    , dex.block_time
    , dex.block_number
    , dex.token_bought_symbol
    , dex.token_sold_symbol
    , dex.token_pair
    , dex.token_bought_amount
    , dex.token_sold_amount
    , dex.token_bought_amount_raw
    , dex.token_sold_amount_raw
    , dex.amount_usd
    , dex.token_bought_address
    , dex.token_sold_address
    , dex.taker
    , dex.maker
    , dex.project_contract_address
    , f.fee
    , dex.tx_hash
    , dex.tx_from
    , dex.tx_to
    , dex.evt_index
FROM
    {{ ref('dex_trades') }} as dex
INNER JOIN
    {{ source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated') }} as f
    ON f.pool = dex.project_contract_address
WHERE
    dex.project = 'uniswap'
    AND dex.version = '3'