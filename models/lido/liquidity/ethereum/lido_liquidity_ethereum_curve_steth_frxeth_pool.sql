{{ config(
    alias = 'curve_steth_frxeth_pool',
     
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe"]\') }}'
    )
}}

{% set project_start_date = '2023-02-22' %} 



with dates as (
    with day_seq as (select (sequence(cast('{{ project_start_date }}' as date), current_date, interval '1' day)) as day)
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

, steth_in as (
select
    DATE_TRUNC('day', evt_block_time) as time,
    sum(cast(value as double))/1e18 as steth_in,
    sum(cast(value as double)/coalesce(r.rate, 1))/1e18 as wsteth_in,
    r.rate
from {{source('erc20_ethereum','evt_Transfer')}} t
 left join wsteth_rate r on DATE_TRUNC('day', evt_block_time) >= r.day and DATE_TRUNC('day', evt_block_time) < r.next_day
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}
 and contract_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 
 and to = 0x4d9f9D15101EEC665F77210cB999639f760F831E
    
group by 1,4
)

, steth_out as (
select
    DATE_TRUNC('day', evt_block_time) as time,
    -sum(cast(value as double))/1e18 as steth_in,
    -sum(cast(value as double)/coalesce(r.rate, 1))/1e18 as wsteth_in,
    rate
from {{source('erc20_ethereum','evt_Transfer')}} t
 left join wsteth_rate r on DATE_TRUNC('day', evt_block_time) >= r.day and DATE_TRUNC('day', evt_block_time) < r.next_day
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}

 and contract_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 
 and "from" = 0x4d9f9D15101EEC665F77210cB999639f760F831E 
    
    
group by 1, 4
)

, daily_balances as (
select time, sum(steth_in) steth_balance, sum(wsteth_in) as wsteth_balance from (
select * from steth_in
union all
select * from steth_out
) group by 1
)

, steth_balances as (
select  time, 
        sum(steth_balance) steth,
        sum(coalesce(wsteth_balance,steth_balance)) as wsteth 
from daily_balances b
group by 1
order by 1
)

, frxeth_in as (
select
    DATE_TRUNC('day', evt_block_time) as time,
    sum(cast(value as double))/1e18 as frxeth_in
from {{source('erc20_ethereum','evt_Transfer')}} t
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}
 and contract_address = 0x5e8422345238f34275888049021821e8e08caa1f 
 and to = 0x4d9f9d15101eec665f77210cb999639f760f831e 
    
group by 1
)

, frxeth_out as (
select
    DATE_TRUNC('day', evt_block_time) as time,
    -sum(cast(value as double))/1e18 as weth_in
from {{source('erc20_ethereum','evt_Transfer')}} t
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}
 and contract_address = 0x5e8422345238f34275888049021821e8e08caa1f
 and "from" = 0x4d9f9d15101eec665f77210cb999639f760f831e 
   
group by 1
)

, frxeth_daily_balances as (
select time, sum(frxeth_in) frxeth_balance
from(
select * from frxeth_in
union all
select * from frxeth_out
) group by 1
)

, frxeth_balances as (
select  time, 
        sum(frxeth_balance) as frxeth
from frxeth_daily_balances b
group by 1
order by 1
)

, frx_eth_rate as (
select call_block_time as time, 
lead(call_block_time, 1, now() + interval '1' day) over (order by call_block_time) as next_time, 
output_0/1e18 as rate
from {{source('curvefi_ethereum','frxeth_eth_pool_call_price_oracle')}}
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', call_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE DATE_TRUNC('day', call_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
 {% endif %}
 and call_success = true

)

, weth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        avg(price) AS price
    FROM {{source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %} 
    and date_trunc('day', minute) < current_date
    and blockchain = 'ethereum'
    and symbol = 'WETH'
    group by 1
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = current_date
    and blockchain = 'ethereum'
    and symbol = 'WETH'
    
    
)    

, steth_prices_hourly AS (
    select time
    , lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time
    , price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time
        , last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
 
    and blockchain = 'ethereum'
    and contract_address = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
    
))   

, steth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        avg(price) AS price
    FROM {{source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
 
    and date_trunc('day', minute) < current_date
    and blockchain = 'ethereum'
    and contract_address = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
    group by 1
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = current_date
    and blockchain = 'ethereum'
    and contract_address = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84

)

, token_exchange_hourly as( 
    select date_trunc('hour', evt_block_time) as time
        , sum(case when cast(sold_id as int) = int '0' then cast(tokens_sold as double) else cast(tokens_bought as double) end) as steth_amount_raw
    from {{source('curvefi_ethereum','frxETH_stETH_evt_TokenExchange')}} c
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', c.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', c.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    group by 1
    
)

, trading_volume_hourly as (
    select t.time
        , t.steth_amount_raw * wp.price as volume_raw 
    from token_exchange_hourly t
    left join steth_prices_hourly wp on t.time = wp.time
    order by 1
)

, trading_volume as ( 
    select distinct date_trunc('day', time) as time
        , sum(volume_raw)/1e18 as volume
    from trading_volume_hourly 
    GROUP by 1
)

select 'ethereum curve frxETH:stETH 0.04' as pool_name, 
0x4d9f9D15101EEC665F77210cB999639f760F831E as pool, 
'ethereum' as blockchain, 'curve' as project,0.04 as fee,
cast(b.time as date) as time, 
0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 as main_token, 'stETH' as main_token_symbol,
0x5E8422345238F34275888049021821E8E08CAa1f as paired_token, 'frxETH' as paired_token_symbol,
wsteth as main_token_reserve,
coalesce(frxeth.frxeth, 0) as paired_token_reserve,
coalesce(stethp.price*r.rate, wethp.price*r.rate) as main_token_usd_price,
wethp.price*coalesce(fr.rate,1) as paired_token_usd_price,
v.volume as trading_volume
from steth_balances b 
left join wsteth_rate r on b.time >= r.day and b.time < r.next_day 
left join frxeth_balances frxeth on b.time = frxeth.time 
left join steth_prices_daily stethp on b.time = stethp.time 
left join weth_prices_daily wethp on b.time = wethp.time 
left join frx_eth_rate fr on b.time >= fr.time and b.time < fr.next_time 
left join trading_volume v on b.time = v.time
order by 1

