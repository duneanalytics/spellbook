{{ config(

    alias = 'curve_wsteth_reth_pool',
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

{% set project_start_date = '2022-02-22' %} 



with dates AS (
        SELECT explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 day)) AS day
    )
 


,wsteth_in as (
select
    DATE_TRUNC('day', evt_block_time) as time,
    sum(cast(value as double))/1e18 as wsteth_in
from {{source('erc20_ethereum','evt_Transfer')}} t
where contract_address = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') and 
    to = lower('0x447Ddd4960d9fdBF6af9a790560d0AF76795CB08') and
    DATE_TRUNC('day', evt_block_time) >= to_date('{{ project_start_date }}')
    
group by 1
)

, wsteth_out as (
select
    DATE_TRUNC('day', evt_block_time) as time,
    -sum(cast(value as double))/1e18 as wsteth_out
from {{source('erc20_ethereum','evt_Transfer')}} t
where contract_address = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') and 
    from = lower('0x447Ddd4960d9fdBF6af9a790560d0AF76795CB08') and
    DATE_TRUNC('day', evt_block_time) >= to_date('{{ project_start_date }}')
    
group by 1
)

, wsteth_daily_balances as (
select time, sum(wsteth_in) wsteth_balance from (
select * from wsteth_in
union all
select * from wsteth_out
) group by 1
)

, wsteth_balances as (
select time, lead(time, 1, now()+ interval 1 day ) over (order by time) as next_time,
sum(wsteth_balance) over (order by time) as wsteth_cumu
from wsteth_daily_balances b
order by 1
)


, reth_in as (
select
    DATE_TRUNC('day', evt_block_time) as time,
    sum(cast(value as double))/1e18 as reth_in
from {{source('erc20_ethereum','evt_Transfer')}} t
where 
    contract_address = lower('0xae78736Cd615f374D3085123A210448E74Fc6393') and 
    to = lower('0x447Ddd4960d9fdBF6af9a790560d0AF76795CB08') and
    DATE_TRUNC('day', evt_block_time) >= to_date('{{ project_start_date }}')
group by 1
)

, reth_out as (
select
    DATE_TRUNC('day', evt_block_time) as time,
    -sum(cast(value as double))/1e18 as reth_out
from {{source('erc20_ethereum','evt_Transfer')}} t
where 
    contract_address = lower('0xae78736Cd615f374D3085123A210448E74Fc6393') and 
    from = lower('0x447Ddd4960d9fdBF6af9a790560d0AF76795CB08') and
    DATE_TRUNC('day', evt_block_time) >= to_date('{{ project_start_date }}')
group by 1
)

, reth_daily_balances as (
select time, sum(reth_in) as reth_balance from (
select * from reth_in
union all
select * from reth_out
) group by 1
)

, reth_balances as (
select time, lead(time, 1, now()+ interval 1 day ) over (order by time) as next_time,
sum(reth_balance) over (order by time) as reth_cumu
from reth_daily_balances 
order by 1
)


, reth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        avg(price) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) >= to_date('{{ project_start_date }}') and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address = lower('0xae78736Cd615f374D3085123A210448E74Fc6393')
    group by 1
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address = lower('0xae78736Cd615f374D3085123A210448E74Fc6393')
    
    
)    

, wsteth_prices_hourly AS (
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
    and contract_address = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0')
    
))   

, wsteth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        avg(price) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) >= to_date('{{ project_start_date }}') and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0')
    group by 1
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0')

)

, token_exchange_hourly as( 
    select date_trunc('hour', evt_block_time) as time
        , sum(case when cast(sold_id as int) = 0 then cast(tokens_sold as double) else cast(tokens_bought as double) end) as eth_amount_raw
    from {{source('curvefi_ethereum','rETHwstETH_evt_TokenExchange')}} c
    group by 1
    
)

, trading_volume_hourly as (
    select t.time
        , t.eth_amount_raw * wp.price as volume_raw 
    from token_exchange_hourly t
    left join wsteth_prices_hourly wp on t.time = wp.time
    order by 1
)

, trading_volume as ( 
    select distinct date_trunc('day', time) as time
        , sum(volume_raw)/1e18 as volume
    from trading_volume_hourly 
    GROUP by 1
)

select 'ethereum curve rETH:wstETH 0.04' as pool_name, lower('0x447Ddd4960d9fdBF6af9a790560d0AF76795CB08') as pool, 
        'ethereum' as blockchain, 'curve' as project,0.04 as fee,
        d.day as time, lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') as main_token, 'wstETH' as main_token_symbol,
         lower('0xae78736Cd615f374D3085123A210448E74Fc6393') as paired_token, 'rETH' as paired_token_symbol,
         wsteth_cumu as main_token_reserve,
         coalesce(reth.reth_cumu, 0) as paired_token_reserve,
         wsteth_cumu*coalesce(wstethp.price, 1)as main_token_usd_reserve,
         coalesce(reth.reth_cumu, 0)*rethp.price as paired_token_usd_reserve,
         v.volume as trading_volume
from dates d
left join wsteth_balances b on d.day >= b.time and d.day < b.next_time 
left join reth_balances reth on d.day >= reth.time and d.day < reth.next_time 
left join wsteth_prices_daily wstethp on d.day = wstethp.time 
left join reth_prices_daily rethp on d.day = rethp.time 
left join trading_volume v on d.day = v.time
order by 1

