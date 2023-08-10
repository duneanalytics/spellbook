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
 
 ref('lido_liquidity_arbitrum_wombat_pools'),
 ref('lido_liquidity_arbitrum_kyberswap_pools'),
 ref('lido_liquidity_arbitrum_uniswap_v3_pools'),
 ref('lido_liquidity_arbitrum_curve_pools'),
 ref('lido_liquidity_arbitrum_balancer_pools')
] %}

{% set project_start_date =  '2021-01-05'%} 


with  dates as (
    with day_seq as (select (sequence(cast('{{ project_start_date }}' as date), cast(now() as date), interval '1' day)) as day)
select days.day
from day_seq
cross join unnest(day) as days(day)
  )

, pools as (  
SELECT *
FROM (
    {% for k_model in lido_liquidity_models %}
    SELECT pool_name, 
           pool, 
           blockchain, 
           project, 
           cast(fee as double) as fee, 
           time, 
           LEAD(time, 1, now() + INTERVAL '1' day) OVER (PARTITION BY pool ORDER BY time NULLS FIRST ) AS next_time,
           main_token, 
           main_token_symbol,
           paired_token, 
           paired_token_symbol, 
           sum(main_token_reserve) over(partition by pool, main_token order by time) as main_token_reserve, 
           sum(paired_token_reserve) over(partition by pool, paired_token order by time) as paired_token_reserve,
           main_token_usd_price*sum(main_token_reserve) over(partition by pool, main_token order by time) as main_token_usd_reserve, 
           paired_token_usd_price*sum(paired_token_reserve) over(partition by pool, paired_token order by time) as paired_token_usd_reserve, 
           trading_volume
    FROM {{ k_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
)

, pools_per_dates as (
  select day, pool
  from dates
  join (select distinct pool from pools) on 1=1
)


SELECT pool_name, 
           l.pool, 
           blockchain, 
           project, 
           fee, 
           d.day as time, 
           main_token, 
           main_token_symbol,
           paired_token, 
           paired_token_symbol, 
           main_token_reserve, 
           paired_token_reserve,
           main_token_usd_reserve, 
           paired_token_usd_reserve, 
           trading_volume
FROM pools_per_dates d
LEFT JOIN pools AS l on d.day >= DATE_TRUNC('day', l.time) and  d.day <  DATE_TRUNC('day', l.next_time) and d.pool = l.pool
WHERE l.pool is not null
;