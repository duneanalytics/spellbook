{{ config(
	tags=['legacy'],
	
    alias = alias('maverick_pools', legacy_model=True),
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

{% set project_start_date = '2023-02-21' %} 

with dates as (
select explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 day)) as day
)

, pools as (
select distinct poolAddress, tokenA, tokenB, fee/1e18 as fee 
from {{source('maverick_v1_ethereum','factory_evt_PoolCreated')}}
where tokenA = lower('0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0') 
   or tokenB = lower('0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0')
)

, pool_per_date as ( 
select dates.day, pools.*
from dates
left join pools on 1=1
)

, tokens as (
select distinct token as address, pt.symbol, pt.decimals 
from (
select tokenA as token
from {{source('maverick_v1_ethereum','factory_evt_PoolCreated')}}
where tokenB = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
union
select tokenB
from {{source('maverick_v1_ethereum','factory_evt_PoolCreated')}}
where tokenA = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
union 
select lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
) t
left join {{ref('prices_tokens_legacy')}} pt on t.token = pt.contract_address
)

, tokens_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        symbol,
        decimals,
        avg(price) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('hour', minute) >= '{{ project_start_date }}'  and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address from tokens)
    group by 1,2,3,4
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        contract_address as token,
        symbol,
        decimals,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address from tokens)
)

, wsteth_prices_hourly as (   
   select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1 hour')) over (order by time) as next_time, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time, 
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('hour', minute) >= '{{ project_start_date }}' 
    and blockchain = 'ethereum'
    and contract_address = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') ) p
)

, swap_events as (
    select 
        date_trunc('day', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.tokenA, cr.tokenB,
        sum(case when tokenAIn then amountIn else (-1)*amountOut end) as amountA,
        sum(case when tokenAIn then (-1)*amountOut else amountIn end) as amountB
    from {{source('maverick_v1_ethereum','pool_evt_Swap')}} sw
    left join {{source('maverick_v1_ethereum','factory_evt_PoolCreated')}} cr on sw.contract_address = cr.poolAddress
    WHERE date_trunc('day', sw.evt_block_time) >= '{{ project_start_date }}' 
    and sw.contract_address in (select poolAddress from pools) 
    group by 1,2,3,4
)

, addliquidity_events as (
    select  date_trunc('day', call_block_time) as time, 
        a.contract_address as pool,
        cr.tokenA, cr.tokenB,
        sum(cast(output_tokenAAmount as double)) as  amountA,
        sum(cast(output_tokenBAmount as double)) as  amountB 
from {{source('maverick_v1_ethereum','pool_call_addLiquidity')}} a
left join {{source('maverick_v1_ethereum','factory_evt_PoolCreated')}} cr on a.contract_address = cr.poolAddress
WHERE date_trunc('day', a.call_block_time) >= '{{ project_start_date }}' 
 and a.call_success 
 and a.contract_address in (select poolAddress from pools)
group by 1,2,3,4
)

, removeliquidity_events as (
select  date_trunc('day', call_block_time) as time, 
        a.contract_address as pool,
        cr.tokenA, cr.tokenB,
        (-1)*sum(cast(output_tokenAOut as double)) as amountA,
        (-1)*sum(cast(output_tokenBOut as double)) as  amountB
from {{source('maverick_v1_ethereum','pool_call_removeLiquidity')}} a
left join {{source('maverick_v1_ethereum','factory_evt_PoolCreated')}} cr on a.contract_address = cr.poolAddress
WHERE date_trunc('day', a.call_block_time) >= '{{ project_start_date }}' 
 and a.call_success 
 and a.contract_address  in (select poolAddress from pools) 
group by 1,2,3,4
)

, daily_delta_balance AS (

select time, pool, tokenA, tokenB, sum(amountA) as amountA, sum(amountB) as amountB
from (
select time, pool,tokenA, tokenB, amountA, amountB
from  swap_events

union all

select time, pool,tokenA, tokenB, amountA, amountB
from  addliquidity_events

union all

select time, pool,tokenA, tokenB, amountA, amountB
from  removeliquidity_events

) group by 1,2,3,4
)

, daily_delta_balance_with_lead AS (
select time, pool, tokenA, tokenB, amountA, amountB, lead(time, 1, now()) over (partition by pool order by time) as next_time
from daily_delta_balance
)


, pool_liquidity as (
SELECT c.day as time, fee,
      c.poolAddress as pool,
      c.tokenA,
      c.tokenB,
      coalesce((SUM(amountA) OVER (PARTITION BY  pool   ORDER BY time)),0) AS amountA,
      coalesce((SUM(amountB) OVER (PARTITION BY  pool   ORDER BY time)),0) AS amountB
    FROM pool_per_date c
      left join daily_delta_balance_with_lead b  ON b.time = c.day and c.poolAddress  = b.pool
)



, wsteth_traded_hourly as (
 select 
        date_trunc('hour', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.tokenA, cr.tokenB,
        sum(case when (cr.tokenA = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') and tokenAIn = true) then amountIn 
                 when (cr.tokenA = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') and tokenAIn = false) then amountOut 
                 when (cr.tokenA != lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') and tokenAIn = true) then amountOut 
                 when (cr.tokenA != lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') and tokenAIn = false) then amountIn 
                 end) as amount
    from {{source('maverick_v1_ethereum','pool_evt_Swap')}} sw
    left join {{source('maverick_v1_ethereum','factory_evt_PoolCreated')}} cr on sw.contract_address = cr.poolAddress
    WHERE date_trunc('day', sw.evt_block_time) >= '{{ project_start_date }}' 
    and sw.contract_address in (select poolAddress from pools)
    group by 1,2,3,4

)

, trading_volume_hourly as (
select t.time, pool, t.amount*wp.price as volume_raw 
from wsteth_traded_hourly t
left join wsteth_prices_hourly wp on date_trunc('hour',t.time) >= wp.time and date_trunc('hour',t.time) < wp.next_time
order by 1,2
)

, trading_volume as ( 
    select distinct date_trunc('day', time) as time
        , pool    
        , sum(volume_raw)/1e18 as volume
    from trading_volume_hourly 
    GROUP by 1,2
)

, all_metrics as (
select   
        o.pool,
        'ethereum' as blockchain,
        'maverick' as project,
        100*fee as fee,
        o.time, 
        case when o.tokenA = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' then o.tokenA else o.tokenB end as main_token, 
        case when o.tokenA = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' then pA.symbol else pB.symbol end as main_token_symbol,
        case when o.tokenA = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' then o.tokenB else o.tokenA end as paired_token, 
        case when o.tokenA = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' then pB.symbol else pA.symbol end as paired_token_symbol,
        case when o.tokenA = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' then amountA/power(10,pA.decimals) else amountB/power(10,pB.decimals) end as main_token_reserve,
        case when o.tokenA = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' then amountB/power(10,pB.decimals) else amountA/power(10,pA.decimals) end as paired_token_reserve,
        case when o.tokenA = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' then amountA*pA.price/power(10,pA.decimals) else amountB*pB.price/power(10,pB.decimals) end as main_token_usd_reserve,
        case when o.tokenA = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' then amountB*pB.price/power(10,pB.decimals) else amountA*pA.price/power(10,pA.decimals) end as paired_token_usd_reserve,
        coalesce(t.volume,0) as trading_volume
from pool_liquidity o
left join tokens_prices_daily pA on o.time = pA.time and o.tokenA = pA.token
left join tokens_prices_daily pB on o.time = pB.time and o.tokenB = pB.token
left join trading_volume t on o.time = t.time and o.pool = t.pool
)


select  CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), coalesce(paired_token_symbol,'unknown')),':') , main_token_symbol, ' ', fee, '(', pool,')') as pool_name,
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
from all_metrics
order by time desc
