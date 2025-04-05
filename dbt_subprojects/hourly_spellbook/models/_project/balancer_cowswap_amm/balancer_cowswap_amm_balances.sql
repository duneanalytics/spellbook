{{
    config(
        schema = 'balancer_cowswap_amm',
        alias = 'balances',
        materialized = 'view'
    )
}}


{% set b_cow_amm_models = [
    ref('balancer_cowswap_amm_arbitrum_balances'),
    ref('balancer_cowswap_amm_base_balances'),
    ref('balancer_cowswap_amm_ethereum_balances'),
    ref('balancer_cowswap_amm_gnosis_balances')
] %}

SELECT *
FROM (
    {% for model in b_cow_amm_models %}
    SELECT
        day, 
        blockchain,
        pool_address, 
        token_address, 
        token_balance_raw
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)