{{ config(
    schema = 'dex_multichain'
    , alias = 'automated_base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
      ref('uniswap_v2_multichain_automated_base_trades')
    , ref('uniswap_v3_multichain_automated_base_trades')
] %}

WITH base_union AS (
    SELECT *
    FROM (
        {% for base_model in base_models %}
        SELECT
            blockchain
            , project
            , project_status
            , version
            , dex_type
            , factory_address
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
        FROM
            {{ base_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

Select 
    model.*
    , tx."from" as tx_from
    , tx."to" as tx_to
    , tx."index" as tx_index
from base_union model
inner join {{source('evms', 'transactions')}} tx
    on model.block_number = tx.block_number
    and model.tx_hash = tx.hash