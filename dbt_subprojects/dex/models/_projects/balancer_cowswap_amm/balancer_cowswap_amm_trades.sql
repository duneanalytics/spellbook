{{
    config(
        schema = 'balancer_cowswap_amm',
        alias = 'trades',
        materialized = 'view'
    )
}}

{% set b_cow_amm_models = [
    ref('balancer_cowswap_amm_arbitrum_trades'),
    ref('balancer_cowswap_amm_base_trades'),
    ref('balancer_cowswap_amm_ethereum_trades'),
    ref('balancer_cowswap_amm_gnosis_trades')
] %}

SELECT *
FROM (
    {% for model in b_cow_amm_models %}
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
       , pool_id
       , tx_hash
       , tx_from
       , tx_to
       , evt_index
       , pool_type
       , pool_symbol
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)