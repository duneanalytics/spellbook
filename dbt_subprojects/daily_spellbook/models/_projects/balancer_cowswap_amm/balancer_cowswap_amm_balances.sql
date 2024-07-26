{{
    config(
        schema = 'balancer_cowswap_amm',
        alias = 'balances',
        materialized = 'view'
    )
}}


{% set b_cow_amm_models = [
    ref('balancer_cowswap_amm_ethereum_balances')
] %}

SELECT *
FROM (
    {% for model in b_cow_amm_models %}
    SELECT
        day, 
        pool_address, 
        token_address, 
        token_balance_raw
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)