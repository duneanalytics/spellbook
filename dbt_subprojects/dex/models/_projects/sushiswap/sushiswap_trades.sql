{{ config(
        schema = 'sushiswap',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(blockchains = \'["ethereum", "gnosis", "avalanche_c", "arbitrum", "fantom", "optimism", "base", "celo", "bnb", "polygon"]\',
                                      spell_type = "project", 
                                      spell_name = "sushiswap", 
                                      contributors = \'["augustog", "hosuke", "Henrystats", "msilb7", "chrispearcx", "codingsh", "wuligy", "tomfutago"]\') }}'
        )
}}


SELECT  blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
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
        , evt_index
FROM {{ ref('dex_trades') }}
WHERE project = 'sushiswap'