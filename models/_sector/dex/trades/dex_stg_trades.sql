{{ config(
    schema = 'dex'
    , alias = 'stg_trades'
    , materialized = 'view'
    )
}}

{% set models = [
    ref('dex_arbitrum_stg_trades')
    , ref('dex_base_stg_trades')
    , ref('dex_bnb_stg_trades')
    , ref('dex_celo_stg_trades')
    , ref('dex_ethereum_stg_trades')
    , ref('dex_optimism_stg_trades')
    , ref('dex_polygon_stg_trades')
] %}


with base_union as (
    SELECT *
    FROM
    (
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
        FROM
            {{ model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)
select
    *
from
    base_union