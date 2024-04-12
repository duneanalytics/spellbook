{{ config(
        alias = 'liquidity',
         
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "optimism", "base", "zksync"]\',
                                "project",
                                "lido_liquidity",
                                \'["pipistrella", "zergil1397", "hosuke"]\') }}'
        )
}}

{% set lido_liquidity_models = [
 
 ref('lido_liquidity_arbitrum_wombat_pools'),
 ref('lido_liquidity_arbitrum_kyberswap_pools'),
 ref('lido_liquidity_arbitrum_kyberswap_v2_pools'),
 ref('lido_liquidity_arbitrum_uniswap_v3_pools'),
 ref('lido_liquidity_arbitrum_curve_pools'),
 ref('lido_liquidity_arbitrum_balancer_pools'),
 ref('lido_liquidity_arbitrum_camelot_pools'),
 ref('lido_liquidity_arbitrum_ramses_pools'),
 ref('lido_liquidity_optimism_kyberswap_pools'),
 ref('lido_liquidity_optimism_kyberswap_v2_pools'),
 ref('lido_liquidity_optimism_uniswap_v3_pools'),
 ref('lido_liquidity_optimism_curve_pools'),
 ref('lido_liquidity_optimism_balancer_pools'),
 ref('lido_liquidity_optimism_velodrome_pools'),
 ref('lido_liquidity_optimism_velodrome_v2_pools'),
 ref('lido_liquidity_polygon_balancer_pools'),
 ref('lido_liquidity_polygon_uniswap_v3_pools'),
 ref('lido_liquidity_polygon_kyberswap_v2_pools'),
 ref('lido_liquidity_ethereum_curve_steth_conc_pool'),
 ref('lido_liquidity_ethereum_curve_steth_frxeth_pool'),
 ref('lido_liquidity_ethereum_curve_steth_pool'),
 ref('lido_liquidity_ethereum_curve_wsteth_reth_pool'),
 ref('lido_liquidity_ethereum_curve_steth_ng_pool'),
 ref('lido_liquidity_ethereum_balancer_pools'),
 ref('lido_liquidity_ethereum_kyberswap_pools'),
 ref('lido_liquidity_ethereum_kyberswap_v2_pools'),
 ref('lido_liquidity_ethereum_maverick_pools'),
 ref('lido_liquidity_ethereum_uniswap_v3_pools'),
 ref('lido_liquidity_ethereum_pancakeswap_v3_pools'),
 ref('lido_liquidity_ethereum_uniswap_v2_pools'),
 ref('lido_liquidity_ethereum_solidly_pools'),
 ref('lido_liquidity_base_kyberswap_pools'),
 ref('lido_liquidity_base_aerodrome_pools'),
 ref('lido_liquidity_base_uniswap_v3_pools'),
 ref('lido_liquidity_zksync_syncswap_pools'),
] %}

{% set project_start_date =  '2020-12-15'%} 


with  dates as (
    with day_seq as (select (sequence(cast('{{ project_start_date }}' as date), cast(now() as date), interval '1' day)) as day)
select days.day
from day_seq
cross join unnest(day) as days(day)
  )

, volumes as (
select u.call_block_time as time,  
cast(output_0 as double) as steth, cast(_wstETHAmount as double) as wsteth 
from  {{source('lido_ethereum','WstETH_call_unwrap')}} u 
where call_success = TRUE 
union all
select u.call_block_time, cast(_stETHAmount as double) as steth, cast(output_0 as double) as wsteth 
from  {{source('lido_ethereum','WstETH_call_wrap')}} u
where call_success = TRUE 
)


, wsteth_rate as (
SELECT
  day, rate as rate0, value_partition, first_value(rate) over (partition by value_partition order by day) as rate,
  lead(day,1,date_trunc('day', now() + interval '1' day)) over(order by day) as next_day
  
FROM (
select day, rate,
sum(case when rate is null then 0 else 1 end) over (order by day) as value_partition
from (
select  date_trunc('day', d.day) as day, 
       sum(cast(steth as double))/sum(cast(wsteth as double))  AS rate
from dates  d
left join volumes v on date_trunc('day', v.time)  = date_trunc('day', d.day) 
group by 1
))

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
  select    dates.day, pool, rate
  from dates
  join (select distinct pool from pools) on 1=1
  left join wsteth_rate on dates.day = wsteth_rate.day
)


SELECT     l.pool_name, 
           l.pool, 
           l.blockchain, 
           l.project, 
           l.fee, 
           d.day as time, 
           l.main_token, 
           l.main_token_symbol,
           l.paired_token, 
           l.paired_token_symbol, 
           case when l.main_token_symbol = 'stETH' and l.pool != 0x4028daac072e492d34a3afdbef0ba7e35d8b55c4 
                then l.main_token_reserve* rate else l.main_token_reserve end as main_token_reserve, 
           l.paired_token_reserve,
           l.main_token_usd_reserve, 
           l.paired_token_usd_reserve, 
           coalesce(vol.trading_volume, 0) as trading_volume
FROM pools_per_dates d
LEFT JOIN pools AS l on d.day >= DATE_TRUNC('day', l.time) and  d.day <  DATE_TRUNC('day', l.next_time) and d.pool = l.pool
LEFT JOIN pools AS vol on d.day = DATE_TRUNC('day', vol.time)  and d.pool = vol.pool
WHERE l.pool is not null  