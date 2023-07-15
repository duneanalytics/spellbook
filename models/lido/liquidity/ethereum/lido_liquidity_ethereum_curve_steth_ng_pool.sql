{{ config(
    alias = alias('curve_steth_ng_pool'),
    partition_by = ['time'],
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe"]\') }}'
    )
}}

{% set project_start_date = '2023-05-16' %} 



with dates AS (
        SELECT explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 day)) AS day
    )
 

, volumes as (
select u.call_block_time as time,  output_0 as steth, _wstETHAmount as wsteth 
from  {{source('lido_ethereum','WstETH_call_unwrap')}} u 
where call_success = TRUE 
union all
select u.call_block_time, _stETHAmount as steth, output_0 as wsteth 
from  {{source('lido_ethereum','WstETH_call_wrap')}} u
where call_success = TRUE 
)


, wsteth_rate as (
SELECT
  day, rate as rate0, value_partition, first_value(rate) over (partition by value_partition order by day) as rate,
  lead(day,1,date_trunc('day', now() + interval 1 day)) over(order by day) as next_day
  
FROM (
select day, rate,
sum(case when rate is null then 0 else 1 end) over (order by day) as value_partition
from (
select  date_trunc('day', d.day) as day, 
        sum(cast(steth as double))/sum(cast(wsteth as double)) AS rate
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
where 
    contract_address = lower('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') and 
    to = lower('0x21e27a5e5513d6e65c4f830167390997aa84843a') and
    DATE_TRUNC('day', evt_block_time) >= to_date('{{ project_start_date }}')
    
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
where 
    contract_address = lower('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') and 
    from = lower('0x21e27a5e5513d6e65c4f830167390997aa84843a') and
    DATE_TRUNC('day', evt_block_time) >= to_date('{{ project_start_date }}')
    
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
select time, 
lead(time, 1, now()+ interval 1 day ) over (order by time) as next_time,
sum(steth_balance) over (order by time) as steth_cumu,
sum(coalesce(wsteth_balance,steth_balance)) over (order by time) as wsteth_cumu, r.rate,
(sum(coalesce(wsteth_balance,steth_balance)) over (order by time))*coalesce(r.rate, 1) as steth_from_wsteth 
from daily_balances b
left join wsteth_rate r on b.time >= r.day and b.time < r.next_day 
order by 1
)

, eth_balances as (
select time, lead(time, 1, date_trunc('day', now() + interval 1 day)) over (order by time) as next_time, sum(eth_amount) over (order by time) as eth_pool
from (
SELECT date_trunc('day', block_time) as time, SUM(amount) as eth_amount
FROM (
    -- outbound transfers
    SELECT block_time, from AS address, -cast(tr.value as double)/1e18 AS amount
    FROM {{source('ethereum','traces')}} tr
    WHERE from = lower('0x21e27a5e5513d6e65c4f830167390997aa84843a')
    AND success
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
    AND DATE_TRUNC('day', block_time) >= to_date('{{ project_start_date }}')

    UNION ALL

    -- inbound transfers
    SELECT block_time, to AS address, cast(value as double)/1e18 AS amount
    FROM {{source('ethereum','traces')}}
    WHERE to = lower('0x21e27a5e5513d6e65c4f830167390997aa84843a')
    AND success
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
    AND DATE_TRUNC('day', block_time) >= to_date('{{ project_start_date }}')

    UNION ALL
    
    -- gas costs
    SELECT block_time, from AS address, -cast(gas_price as double)*cast(gas_used as double)/1e18 
    FROM {{source('ethereum','transactions')}} et
    WHERE from = lower('0x21e27a5e5513d6e65c4f830167390997aa84843a')
    AND DATE_TRUNC('day', block_time) >= to_date('{{ project_start_date }}')
    and success
) t
group by 1
) eth order by 1 desc

) 

, weth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        avg(price) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) >= to_date('{{ project_start_date }}') and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and symbol = 'WETH'
    group by 1
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and symbol = 'WETH'
    
    
)    

, weth_prices_hourly AS (
    select time
    , lead(time,1, DATE_TRUNC('hour', now() + interval 1 hour)) over (order by time) as next_time
    , price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time
        , last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('hour', minute) >= to_date('{{ project_start_date }}')
    and blockchain = 'ethereum'
    and symbol = 'WETH'
    
))   

, steth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        avg(price) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) >= to_date('{{ project_start_date }}') and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address = lower('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
    group by 1
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address = lower('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')

)

, token_exchange_hourly as( 
    select date_trunc('hour', evt_block_time) as time
        , sum(case when cast(sold_id as int) = 0 then cast(tokens_sold as double) else cast(tokens_bought as double) end) as eth_amount_raw
    from {{source('curvefi_ethereum','stETHng_evt_TokenExchange')}} c
    group by 1
    
)

, trading_volume_hourly as (
    select t.time
        , t.eth_amount_raw * wp.price as volume_raw 
    from token_exchange_hourly t
    left join weth_prices_hourly wp on t.time = wp.time
    order by 1
)

, trading_volume as ( 
    select distinct date_trunc('day', time) as time
        , sum(volume_raw)/1e18 as volume
    from trading_volume_hourly 
    GROUP by 1
)



select 'ethereum curve ETH:stETH NG 0.04' as pool_name, '0x21e27a5e5513d6e65c4f830167390997aa84843a' as pool, 'ethereum' as blockchain, 'curve' as project,0.04 as fee,
        d.day as time, '0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84' as main_token, 'stETH' as main_token_symbol,
         '0x0000000000000000000000000000000000000000' as paired_token, 'ETH' as paired_token_symbol,
         steth_from_wsteth as main_token_reserve,
         coalesce(eth.eth_pool, 0) as paired_token_reserve,
         steth_from_wsteth*coalesce(stethp.price, wethp.price)as main_token_usd_reserve,
         coalesce(eth.eth_pool, 0)*wethp.price as paired_token_usd_reserve,
         v.volume as trading_volume
from dates d
left join steth_balances b on d.day >= b.time and d.day < b.next_time
left join eth_balances eth on d.day >= eth.time and d.day < eth.next_time 
left join steth_prices_daily stethp on d.day = stethp.time 
left join weth_prices_daily wethp on d.day = wethp.time 
left join trading_volume v on d.day = v.time
order by 1
