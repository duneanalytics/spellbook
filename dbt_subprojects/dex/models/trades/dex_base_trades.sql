{{ config(
    schema = 'dex'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set models = [  
      ref('dex_ethereum_base_trades')
    , ref('dex_hemi_base_trades')
   
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
        , token_bought_amount_raw
        , token_sold_amount_raw
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , evt_index
        , tx_from
        , tx_to
        , tx_index
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