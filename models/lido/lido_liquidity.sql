{{ config(
        alias = alias('liquidity'),
        tags = ['dunesql'], 
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "optimism"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe", "gregshestakovlido", "hosuke"]\') }}'
        )
}}

{% set lido_liquidity_models = [
 
 ref('lido_liquidity_arbitrum_wombat_pools')
 
] %}


SELECT *
FROM (
    {% for k_model in lido_liquidity_models %}
    SELECT pool_name, 
           pool, 
           blockchain, 
           project, 
           cast(fee as double) as fee, 
           time, 
           main_token, 
           main_token_symbol,
           paired_token, 
           paired_token_symbol, 
           sum(main_token_reserve) over(partition by pool, main_token order by time) as main_token_reserve, 
           sum(paired_token_reserve) over(partition by pool, paired_token order by time) as paired_token_reserve,
           sum(main_token_usd_reserve) over(partition by pool, main_token order by time) as main_token_usd_reserve, 
           sum(paired_token_usd_reserve) over(partition by pool, paired_token order by time) as paired_token_usd_reserve, 
           trading_volume
    FROM {{ k_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;