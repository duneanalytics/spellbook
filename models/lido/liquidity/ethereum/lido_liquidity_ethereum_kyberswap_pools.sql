{{ config(
    alias = 'kyberswap_pools',
    partition_by = ['time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe", "gregshestakovlido"]\') }}'
    )
}}

{% set project_start_date = '2022-10-28' %} 

with dates as (
select explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 hour)) as hour
)
 
, pools as (
select pool as address, 'ethereum' as blockchain, 'kyberswap' as project, swapFeeUnits/1000 as fee
from {{ source('kyber_ethereum', 'Elastic_Factory_evt_PoolCreated') }}
where token0 = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') or token1 = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0')
)

, tokens as (
select distinct token as address, pt.symbol, pt.decimals 
from (
select token1 as token
from {{ source('kyber_ethereum', 'Elastic_Factory_evt_PoolCreated') }}
where token0 = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
union
select token0
from {{ source('kyber_ethereum', 'Elastic_Factory_evt_PoolCreated') }}
where token1 = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
union 
select lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') 
) t
left join {{ref('prices_tokens')}} pt on t.token = pt.contract_address
)

, tokens_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        avg(price) AS price
    FROM {{ source('prices', 'usd') }}
    {% if is_incremental() %}
    WHERE date_trunc('day', minute) >= date_trunc("day", now() - interval '1 week') and date_trunc('day', minute) < date_trunc('day', now())
    {% else %}
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    {% endif %}
    and blockchain = 'ethereum'
    and contract_address in (select address from tokens)    
    group by 1,2
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        contract_address as token,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address from tokens)
)

, tokens_prices_hourly AS (
    select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1 hour')) over (partition by token order by time) as next_time, token, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time, 
        contract_address as token,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices', 'usd') }}
    {% if is_incremental() %}
    WHERE date_trunc('hour', minute) >= date_trunc("hour", now() - interval '7 days')
    {% else %}
    WHERE date_trunc('hour', minute) >= '{{ project_start_date }}' 
    {% endif %} 
    and blockchain = 'ethereum'
    and contract_address in (select address from tokens)   
) p
)


, swap_events as (
    select 
        date_trunc('day', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        sum(cast(deltaQty0 as DOUBLE)) as amount0,
        sum(cast(deltaQty1 as DOUBLE)) as amount1
        
    from {{ source('kyber_ethereum', 'Elastic_Pool_evt_swap') }} sw
    left join {{ source('kyber_ethereum', 'Elastic_Factory_evt_PoolCreated') }} cr on sw.contract_address = cr.pool
    {% if is_incremental() %}
    WHERE date_trunc('day', sw.evt_block_time) >= date_trunc("day", now() - interval '1 week') 
    {% else %}
    WHERE date_trunc('day', sw.evt_block_time) >= '{{ project_start_date }}'
    {% endif %} 
    and sw.contract_address in (select address from pools)   
    group by 1,2,3,4
) 
    
, mint_events as (
    select 
        date_trunc('day', mt.evt_block_time) as time,
        mt.contract_address as pool,
        cr.token0, cr.token1,
        sum(cast(qty0 as DOUBLE)) as amount0,
        sum(cast(qty1 as DOUBLE)) as amount1
    from {{ source('kyber_ethereum', 'Elastic_Pool_evt_Mint') }} mt
    left join {{ source('kyber_ethereum', 'Elastic_Factory_evt_PoolCreated') }} cr on mt.contract_address = cr.pool
    {% if is_incremental() %}
    WHERE date_trunc('day', mt.evt_block_time) >= date_trunc("day", now() - interval '1 week') 
    {% else %}
    WHERE date_trunc('day', mt.evt_block_time) >= '{{ project_start_date }}'
    {% endif %}
    and mt.contract_address in (select address from pools)    
    group by 1,2,3,4
    union all
    select d.day as time, cr.pool, cr.token0, cr.token1, 0, 0
    from (select distinct date_trunc('day', hour) as day from dates) d
    left join {{ source('kyber_ethereum', 'Elastic_Factory_evt_PoolCreated') }} cr on 1 = 1
    where cr.pool in (select address from pools)
)
    
, burn_events as (
    select 
        date_trunc('day', bn.evt_block_time) as time,
        bn.contract_address as pool,
        cr.token0, cr.token1,
        (-1)*sum(cast(qty0 as DOUBLE)) as amount0,
        (-1)*sum(cast(qty1 as DOUBLE)) as amount1
    from {{ source('kyber_ethereum', 'Elastic_Pool_evt_Burn') }} bn
    left join {{ source('kyber_ethereum', 'Elastic_Factory_evt_PoolCreated') }} cr on bn.contract_address = cr.pool
    {% if is_incremental() %}
    WHERE date_trunc('day', bn.evt_block_time) >= date_trunc("day", now() - interval '1 week') 
    {% else %}
    WHERE date_trunc('day', bn.evt_block_time) >= '{{ project_start_date }}'
    {% endif %}
    and bn.contract_address in (select address from pools)    
    group by 1,2,3,4

    union all

    select 
        date_trunc('day', bn.evt_block_time), 
        bn.contract_address as pool,
        cr.token0, cr.token1,
        (-1) * sum(cast(qty0 as double)) as amount0, 
        (-1) * sum(cast(qty1 as double)) as amount1 
    from {{ source('kyber_ethereum', 'Elastic_Pool_evt_BurnRTokens') }} bn
    left join {{ source('kyber_ethereum', 'Elastic_Factory_evt_PoolCreated') }} cr on bn.contract_address = cr.pool
    {% if is_incremental() %}
    WHERE date_trunc('day', bn.evt_block_time) >= date_trunc("day", now() - interval '1 week') 
    {% else %}
    WHERE date_trunc('day', bn.evt_block_time) >= '{{ project_start_date }}'
    {% endif %}
    and bn.contract_address in (select address from pools)    
    group by 1,2,3,4
)

    
, daily_delta_balance as (
    select time, pool, token0, token1, sum(coalesce(amount0, 0)) as amount0, sum(coalesce(amount1, 0)) as amount1
    from ( 
    select time, pool, token0, token1, amount0, amount1 
    from swap_events
    union all
    select time, pool, token0, token1, amount0, amount1 
    from mint_events
    union all
    select time, pool, token0, token1, amount0, amount1 
    from burn_events
    ) balance
    group by 1,2,3,4
)
  
, pool_liquidity as (
    select  time, lead(time, 1, current_date + interval '1 day') over (order by time) as next_time, 
            pool, token0, token1, sum(amount0) over(partition by pool order by time) as amount0, sum(amount1)  over(partition by pool order by time) as amount1
    from daily_delta_balance
)


, swap_events_hourly as (
    select hour, pool, token0, token1, sum(amount0) as amount0, sum(amount1) as amount1 from (
    select 
        d.hour,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        coalesce(sum(cast(abs(deltaQty0) as DOUBLE)),0) as amount0,
        coalesce(sum(cast(abs(deltaQty1) as DOUBLE)),0) as amount1
        
    from dates d
    left join {{source('kyber_ethereum','Elastic_Pool_evt_swap')}} sw on d.hour = date_trunc('hour', sw.evt_block_time)
    left join {{source('kyber_ethereum','Elastic_Factory_evt_PoolCreated')}} cr on sw.contract_address = cr.pool
    where sw.contract_address in (select address from pools)
    group by 1,2,3,4
    union all
    select d.hour,
        cr.pool as pool,
        cr.token0, cr.token1, 0, 0
    from dates d
    left join {{source('kyber_ethereum','Elastic_Factory_evt_PoolCreated')}} cr on 1 = 1
    where cr.pool in (select address from pools)  
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
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then amount0/power(10, t0.decimals)  else amount1/power(10, t1.decimals)  end main_token_reserve,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then amount1/power(10, t1.decimals)  else amount0/power(10, t0.decimals)  end paired_token_reserve,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then p0.price*amount0/power(10, t0.decimals) else p1.price*amount1/power(10, t1.decimals) end as main_token_usd_reserve,
    case when token0 = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') then p1.price*amount1/power(10, t1.decimals) else p0.price*amount0/power(10, t0.decimals) end as paired_token_usd_reserve,
    volume as trading_volume
from pool_liquidity l 
left join pools on l.pool = pools.address
left join tokens t0 on l.token0 = t0.address
left join tokens t1 on l.token1 = t1.address
left join tokens_prices_daily p0 on l.time = p0.time and l.token0 = p0.token
left join tokens_prices_daily p1 on l.time = p1.time and l.token1 = p1.token
left join trading_volume tv on l.time = tv.time and l.pool = tv.pool
) 

select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), paired_token_symbol),':') , main_token_symbol, ' ', fee) as pool_name,* 
from all_metrics
