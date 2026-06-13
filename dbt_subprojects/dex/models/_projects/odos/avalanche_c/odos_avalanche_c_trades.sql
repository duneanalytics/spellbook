{{ config(
        schema = 'odos_avalanche_c',
        alias = 'trades'
        , post_hook='{{ hide_spells() }}'
        )
}}

{% set odos_models = [
  ref('odos_v1_avalanche_c_trades'),
  ref('odos_v2_avalanche_c_trades')
] %}

select *
from (
    {% for aggregator_model in odos_models %}
    select
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
    from {{ aggregator_model }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
