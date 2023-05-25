{{ config(
        alias ='liquidity',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe", "gregshestakovlido", "hosuke"]\') }}'
        )
}}

{% set lido_liquidity_models = [
 ref('lido_liquidity_ethereum_kyberswap_pools')
] %}


SELECT *
FROM (
    {% for k_model in lido_liquidity_models %}
    SELECT pool_name, 
           pool, 
           blockchain, 
           project, 
           fee, 
           time, 
           main_token, 
           main_token_symbol,
           paired_token, 
           paired_token_symbol, 
           main_token_reserve, 
           paired_token_reserve,
           main_token_usd_reserve, 
           paired_token_usd_reserve, 
           trading_volume
    FROM {{ k_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;