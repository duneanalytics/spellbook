{{ config(
	tags=['legacy'],
	
    alias = alias('uniswap_v3_pools', legacy_model=True),
    partition_by = ['time'],
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe", "gregshestakovlido"]\') }}'
    )
}}

{% set project_start_date = '2021-05-05' %} 

with dates as (
select explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 day)) as day
)

, pools as (
select pool as address, 'ethereum' as blockchain, 'uniswap_v3' as project, fee/10000 as fee
from {{source('uniswap_v3_ethereum','Factory_evt_PoolCreated')}}
where token0 = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') or token1 = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0')
)

, pool_per_date as ( 
select dates.day, pools.*
from dates
left join pools on 1=1
)

, tokens as (
select distinct token as address, pt.symbol, pt.decimals 
from (
select token1 as token
from {{source('uniswap_v3_ethereum','Factory_evt_PoolCreated')}}
where token0 = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
union
select token0
from {{source('uniswap_v3_ethereum','Factory_evt_PoolCreated')}}
where token1 = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
union 
select lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
) t
left join prices.tokens pt on t.token = pt.contract_address
)

, tokens_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        avg(price) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE date_trunc('hour', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address from tokens)
    group by 1,2
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        contract_address as token,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address from tokens)
)

, tokens_prices_hourly as (   
   select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1 hour')) over (partition by token order by time) as next_time, token, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time, 
        contract_address as token,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}}
    WHERE date_trunc('hour', minute) >= '{{ project_start_date }}' 
    and blockchain = 'ethereum'
    and contract_address in (select address from tokens)) p
)

, swap_events as (
    select 
        date_trunc('day', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        sum(cast(amount0 as DOUBLE)) as amount0,
        sum(cast(amount1 as DOUBLE)) as amount1
        
    from {{source('uniswap_v3_ethereum','Pair_evt_Swap')}} sw
    left join {{source('uniswap_v3_ethereum','Factory_evt_PoolCreated')}} cr on sw.contract_address = cr.pool
    WHERE date_trunc('day', sw.evt_block_time) >= '{{ project_start_date }}'
    and sw.contract_address in (select address from pools)
    group by 1,2,3,4
)

, mint_events as (
    select 
        date_trunc('day', mt.evt_block_time) as time,
        mt.contract_address as pool,
        cr.token0, cr.token1,
        sum(cast(amount0 as DOUBLE)) as amount0,
        sum(cast(amount1 as DOUBLE)) as amount1
    from {{source('uniswap_v3_ethereum','Pair_evt_Mint')}} mt
    left join {{source('uniswap_v3_ethereum','Factory_evt_PoolCreated')}} cr on mt.contract_address = cr.pool
    WHERE date_trunc('day', mt.evt_block_time) >= '{{ project_start_date }}'
    and mt.contract_address  in (select address from pools)
    group by 1,2,3,4
    
)

, collect_events as (
    select 
        c.evt_block_time as time,
        c.contract_address as pool,
        cr.token0, cr.token1,
        (-1)*cast(amount0 as DOUBLE) as amount0,
        (-1)*cast(amount1 as DOUBLE) as amount1,
        c.evt_tx_hash
    from {{source('uniswap_v3_ethereum','Pair_evt_Collect')}} c
    left join {{source('uniswap_v3_ethereum','Factory_evt_PoolCreated')}} cr on c.contract_address = cr.pool
    WHERE date_trunc('day', c.evt_block_time) >= '{{ project_start_date }}'
    and c.contract_address  in (select address from pools)
    
)


, burn_events as (
    select 
        date_trunc('day', bn.evt_block_time) as time,
        bn.contract_address as pool,
        cr.token0, cr.token1,
        (-1)*sum(cast(amount0 as DOUBLE)) as amount0,
        (-1)*sum(cast(amount1 as DOUBLE)) as amount1
    from {{source('uniswap_v3_ethereum','Pair_evt_Burn')}} bn
    left join {{source('uniswap_v3_ethereum','Factory_evt_PoolCreated')}} cr on bn.contract_address = cr.pool
    WHERE date_trunc('day', bn.evt_block_time) >= '{{ project_start_date }}'
    and bn.contract_address  in (select address from pools)
    and bn.evt_tx_hash not in (select evt_tx_hash from collect_events)
    group by 1,2,3,4
)

, daily_delta_balance as (
    select time, pool, token0, token1, sum(coalesce(amount0, 0)) as amount0, sum(coalesce(amount1, 0)) as amount1,
    lead(time, 1, now() + interval '1 day') over (partition by pool order by time) as next_time
    from ( 
    select time, pool, token0, token1, amount0, amount1 
    from swap_events
    union all
    select time, pool, token0, token1, amount0, amount1 
    from mint_events
    union all
    select time, pool, token0, token1, amount0, amount1 
    from burn_events
    union all
    select date_trunc('day', time), pool, token0, token1, sum(amount0), sum(amount1) 
    from collect_events
    group by 1,2,3,4
    
    ) balance
    group by 1,2,3,4
)

, pool_liquidity as (
    select c.day as time, c.address as pool, token0, token1, sum(amount0) over(partition by pool order by time) as amount0, 
    sum(amount1)  over(partition by pool order by time) as amount1
    from pool_per_date  c
    LEFT JOIN daily_delta_balance b ON c.address = b.pool and  b.time <= c.day AND c.day < b.next_time
)

, swap_events_hourly as (  
    select hour, pool, token0, token1, sum(amount0) as amount0, sum(amount1) as amount1 from (
    select 
        date_trunc('hour', sw.evt_block_time) as hour,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        coalesce(sum(cast(abs(amount0) as DOUBLE)),0) as amount0,
        coalesce(sum(cast(abs(amount1) as DOUBLE)),0) as amount1
        
    from {{source('uniswap_v3_ethereum','Pair_evt_Swap')}} sw 
    left join {{source('uniswap_v3_ethereum','Factory_evt_PoolCreated')}} cr on sw.contract_address = cr.pool
    WHERE date_trunc('day', sw.evt_block_time) >= '{{ project_start_date }}'
    and sw.contract_address in (select address from pools)
    group by 1,2,3,4
      ) a group by 1,2,3,4
) 

, trading_volume_hourly as (
    select hour as time, pool, token0, amount0, p.price, coalesce(p.price*amount0/power(10, t.decimals),0) as volume
    from swap_events_hourly s 
    left join tokens t on s.token0 = t.address
    left join tokens_prices_hourly p on  s.hour >= p.time and s.hour < p.next_time  and s.token0 = p.token
   
)


, trading_volume as (
select  distinct date_trunc('day', time) as time, pool, sum(volume) as volume
from trading_volume_hourly
group by 1,2
)

, all_metrics as (
select l.pool, pools.blockchain, pools.project, pools.fee, l.time, 
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then token0 else token1 end main_token,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then t0.symbol else t1.symbol end main_token_symbol,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then token1 else token0 end paired_token,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then t1.symbol else t0.symbol end paired_token_symbol, 
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then (case when amount0 > 0 then amount0/power(10, t0.decimals) else 0 end)  
          else (case when amount1 > 0 then amount1/power(10, t1.decimals) else 0 end)  
         end main_token_reserve,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then (case when amount1 > 0 then amount1/power(10, t1.decimals) else 0 end)  
          else (case when amount0 > 0 then  amount0/power(10, t0.decimals) else 0 end)
         end paired_token_reserve,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then (case when amount0 > 0 then p0.price*amount0/power(10, t0.decimals) else 0 end)
         else (case when amount1 > 0 then p1.price*amount1/power(10, t1.decimals) else 0 end)
         end as main_token_usd_reserve,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then (case when amount1 > 0 then p1.price*amount1/power(10, t1.decimals) else 0 end)
         else (case when amount0 > 0 then p0.price*amount0/power(10, t0.decimals) else 0 end)
         end as paired_token_usd_reserve,     
    volume as trading_volume
from pool_liquidity l 
left join pools on l.pool = pools.address
left join tokens t0 on l.token0 = t0.address
left join tokens t1 on l.token1 = t1.address
left join tokens_prices_daily p0 on l.time = p0.time and l.token0 = p0.token
left join tokens_prices_daily p1 on l.time = p1.time and l.token1 = p1.token
left join trading_volume tv on l.time = tv.time and l.pool = tv.pool
) 


select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), coalesce(paired_token_symbol,'unknown')),':') , main_token_symbol, ' ', fee) as pool_name,* 
from all_metrics
where main_token_reserve > 1