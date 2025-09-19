{{ config(
        schema = 'angstrom',
        alias = 'trades',
        post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                      spell_type = "project", 
                                      spell_name = "angstrom", 
                                      contributors = \'["jnoorchashm37"]\') }}'
        )
}}

{% set angstrom_models = [
ref('angstrom_ethereum_trades')
] %}


select *
from (
    {% for dex_trade_model in angstrom_models %}
    select
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
        , tx_from
        , tx_to
        , evt_index
        , token_sold_lp_fees_paid_raw
        , token_bought_lp_fees_paid_raw
        , token_sold_protocol_fees_paid_raw
        , token_bought_protocol_fees_paid_raw
        , token_sold_lp_fees_paid
        , token_bought_lp_fees_paid
        , token_sold_protocol_fees_paid
        , token_bought_protocol_fees_paid
        , token_sold_lp_fees_paid_usd
        , token_bought_lp_fees_paid_usd
        , token_sold_protocol_fees_paid_usd
        , token_bought_protocol_fees_paid_usd
    from 
    {{ dex_trade_model }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)