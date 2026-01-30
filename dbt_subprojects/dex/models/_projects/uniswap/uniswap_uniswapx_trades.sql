{{ config(
    schema = 'uniswap'
    , alias = 'uniswapx_trades'
    , materialized = 'view'
    , post_hook='{{ hide_spells() }}')
}}


{% set models = [  
      ref('uniswap_arbitrum_uniswapx_trades')
      , ref('uniswap_base_uniswapx_trades')
      , ref('uniswap_ethereum_uniswapx_trades')
      , ref('uniswap_unichain_uniswapx_trades')
] %}

with base_union as (
    {% for model in models %}
    SELECT
        blockchain
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
        , evt_index
        , tx_from
        , tx_to
    FROM
        {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
select
    *
from
base_union