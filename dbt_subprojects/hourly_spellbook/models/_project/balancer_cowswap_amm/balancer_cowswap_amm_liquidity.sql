{{
    config(
        schema='balancer_cowswap_amm',
        alias = 'liquidity',     
        materialized = 'view'
    )
}}

{% set b_cow_amm_models = [
    ref('balancer_cowswap_amm_ethereum_liquidity'),
    ref('balancer_cowswap_amm_gnosis_liquidity')
] %}

SELECT *
FROM (
    {% for model in b_cow_amm_models %}
    SELECT
            day,
            pool_id,
            pool_address,
            pool_symbol,
            version,
            blockchain,
            pool_type,
            token_address,
            token_symbol,
            token_balance_raw,
            token_balance,
            protocol_liquidity_usd,
            protocol_liquidity_eth,
            pool_liquidity_usd,
            pool_liquidity_eth         
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)