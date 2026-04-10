{{
    config(
        schema = 'oneinch_lop_ethereum',
        alias = 'base_trades',
        materialized = 'view'
    )
}}

SELECT
    blockchain
    , project
    , version
    , block_month
    , block_date
    , block_time
    , block_number
    , cast(token_bought_amount_raw as uint256) as token_bought_amount_raw
    , cast(token_sold_amount_raw as uint256) as token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , evt_index
FROM {{ ref('oneinch_lop_own_trades') }}
WHERE blockchain = 'ethereum'
