{{ config(
    schema='lido_liquidity_arbitrum',
    alias = 'camelot_v3_pools',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.time')],
    post_hook='{{ expose_spells(blockchains = \'["arbitrum"]\',
                                spell_type = "project",
                                spell_name = "lido_liquidity",
                                contributors = \'["pipistrella"]\') }}'
    )
}}

{% set project_start_date = '2023-07-03' %} 

with

 pools as (
select output_pool as address, 'arbitrum' as blockchain, 'camelot' as project, 0 as fee
from {{ source('camelot_v3_arbitrum','AlgebraFactory_call_createPool')}}
where tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529
   or tokenB = 0x5979D7b546E38E414F7E9822514be443A4800529
)


, tokens as (
select distinct token as address
from (
select tokenB as token
from {{ source('camelot_v3_arbitrum','AlgebraFactory_call_createPool')}}
where tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529
union
select tokenA
from {{ source('camelot_v3_arbitrum','AlgebraFactory_call_createPool')}}
where tokenB = 0x5979D7b546E38E414F7E9822514be443A4800529
union 
select 0x5979D7b546E38E414F7E9822514be443A4800529
) t
)



, tokens_prices_daily AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS time,
      contract_address  AS token,
      decimals, 
      symbol,
      AVG(price) AS price
    FROM
      {{source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}    
    WHERE {{ incremental_predicate('p.minute') }}
    {% endif %}
      AND DATE_TRUNC('day', minute) < CURRENT_DATE
      AND blockchain = 'arbitrum'
      AND contract_address IN (SELECT address  FROM tokens)
    GROUP BY 1, 2,3,4
    UNION ALL
    SELECT DISTINCT
      DATE_TRUNC('day', minute),
      contract_address  AS token,
      decimals, 
      symbol,
      LAST_VALUE(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          contract_address
        ORDER BY
          minute NULLS FIRST range BETWEEN UNBOUNDED preceding
          AND UNBOUNDED following
      ) AS price
    FROM
      {{source('prices','usd')}}
    WHERE
      DATE_TRUNC('day', minute) = CURRENT_DATE
      AND blockchain = 'arbitrum'
      AND contract_address IN (SELECT address  FROM tokens)
  ),
  
  tokens_prices_hourly AS (
        SELECT DISTINCT
          DATE_TRUNC('hour', minute) AS time,
          LEAD(DATE_TRUNC('hour', minute),1,DATE_TRUNC('hour', now() + INTERVAL '1' hour)) OVER (PARTITION BY contract_address  ORDER BY DATE_TRUNC('hour', minute) NULLS FIRST) AS next_time,
          contract_address  AS token,
          decimals, 
          symbol,
          LAST_VALUE(price) OVER (
            PARTITION BY
              DATE_TRUNC('hour', minute),
              contract_address
            ORDER BY
              minute NULLS FIRST range BETWEEN UNBOUNDED preceding
              AND UNBOUNDED following
          ) AS price
        FROM
          {{source('prices','usd')}} p
        {% if not is_incremental() %}
        WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
        {% else %}        
        WHERE {{ incremental_predicate('p.minute') }}
        {% endif %}
          AND blockchain = 'arbitrum'
          AND contract_address IN (
            SELECT
              address
            FROM
              tokens
          )
      
  )
  


, swap_events as (
    select 
        date_trunc('day', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.tokenA, cr.tokenB,
        sum(cast(amount0 as DOUBLE))  as amount0,
        sum(cast(amount1 as DOUBLE))  as amount1
        
    from {{source('camelot_v3_arbitrum','Algebrapool_evt_Swap')}} sw
    left join {{source('camelot_v3_arbitrum','AlgebraFactory_call_createPool')}} cr on sw.contract_address = cr.output_pool
  
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('sw.evt_block_time') }}
    {% endif %}
    and sw.contract_address in (select address from pools)

    group by 1,2,3,4
)

, mint_events as (
    select 
        date_trunc('day', mt.evt_block_time) as time,
        mt.contract_address as pool,
        cr.tokenA, cr.tokenB,
        sum(cast(amount0 as DOUBLE)) as amount0,
        sum(cast(amount1 as DOUBLE)) as amount1
    from {{source('camelot_v3_arbitrum','Algebrapool_evt_Mint')}} mt
    left join {{source('camelot_v3_arbitrum','AlgebraFactory_call_createPool')}} cr on mt.contract_address = cr.output_pool
    
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', mt.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('mt.evt_block_time') }}
    {% endif %}
    and mt.contract_address  in (select address from pools)
    group by 1,2,3,4
    
)

, collect_events as (
    select 
        date_trunc('day', bn.evt_block_time) as time,
        bn.contract_address as pool,
        cr.tokenA, cr.tokenB,
        (-1)*sum(cast(amount0 as DOUBLE)) as amount0,
        (-1)*sum(cast(amount1 as DOUBLE)) as amount1
    from {{source('camelot_v3_arbitrum','Algebrapool_evt_Collect')}} bn
    left join {{source('camelot_v3_arbitrum','AlgebraFactory_call_createPool')}} cr on bn.contract_address = cr.output_pool
    
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', bn.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('bn.evt_block_time') }}
    {% endif %}
    and bn.contract_address  in (select address from pools)     
    group by 1,2,3,4
)

, daily_delta_balance as (
    select time, pool, tokenA, tokenB, sum(coalesce(amount0, 0)) as amount0, sum(coalesce(amount1, 0)) as amount1,
    lead(time, 1, now() + interval '1' day) over (partition by pool order by time) as next_time
    from ( 
    select time, pool, tokenA, tokenB, amount0, amount1 
    from swap_events
    union all
    select time, pool, tokenA, tokenB, amount0, amount1 
    from mint_events
    union all
    select time, pool, tokenA, tokenB, amount0, amount1 
    from collect_events
    ) balance
    group by 1,2,3,4
)

, pool_liquidity as (
    SELECT distinct
      time,
      LEAD(time, 1, CURRENT_DATE + INTERVAL '1' day) OVER (ORDER BY time NULLS FIRST) AS next_time,
      pool,
      tokenA,
      tokenB,
      amount0,
      amount1
    FROM ( 
      SELECT 
      time,
      pool,
      d.tokenA,
      d.tokenB,
      SUM(amount0) AS amount0,
      SUM(amount1) AS amount1
    FROM  daily_delta_balance d --on c.address = d.pool and c.day >= d.time and c.day < d.next_time
    GROUP BY 1,2,3,4
    )

    )

, swap_events_hourly as (  
    select hour, pool, tokenA, tokenB, sum(amount0) as amount0, sum(amount1) as amount1 from (
    select 
        date_trunc('hour', sw.evt_block_time) as hour,
        sw.contract_address as pool,
        cr.tokenA, cr.tokenB,
        coalesce(sum(cast(abs(amount0) as DOUBLE)),0)  as amount0,
        coalesce(sum(cast(abs(amount1) as DOUBLE)),0)  as amount1
        
    from {{source('camelot_v3_arbitrum','Algebrapool_evt_Swap')}} sw 
    left join {{source('camelot_v3_arbitrum','AlgebraFactory_call_createPool')}} cr on sw.contract_address = cr.output_pool
    
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}  
    WHERE {{ incremental_predicate('sw.evt_block_time') }}
    {% endif %}

    and sw.contract_address in (select address from pools)
    group by 1,2,3,4
      ) a group by 1,2,3,4
) 

, trading_volume_hourly as (
    select hour as time, pool, tokenA, amount0, p.price, coalesce(p.price*abs(amount0)/power(10, p.decimals),0) as volume
    from swap_events_hourly s 
    left join tokens t on s.tokenA = t.address
    left join tokens_prices_hourly p on  s.hour >= p.time and s.hour < p.next_time  and s.tokenA = p.token
   
)


, trading_volume as (
select  distinct date_trunc('day', time) as time, pool, sum(volume) as volume
from trading_volume_hourly
group by 1,2
)

, all_metrics as (
select l.pool, pools.blockchain, pools.project, pools.fee, cast(l.time as date) as time, 
    case when l.tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529 then l.tokenA else l.tokenB end main_token,
    case when l.tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529 then p0.symbol else p1.symbol end main_token_symbol,
    case when l.tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529 then l.tokenB else l.tokena end paired_token,
    case when l.tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529 then p1.symbol else p0.symbol end paired_token_symbol, 
    case when l.tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529 then amount0/power(10, p0.decimals)  
          else amount1/power(10, p1.decimals) 
         end main_token_reserve,
    case when tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529 then amount1/power(10, p1.decimals) 
          else amount0/power(10, p0.decimals) 
         end paired_token_reserve,
    case when tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529 then p0.price
         else p1.price
         end as main_token_usd_price,
    case when tokenA = 0x5979D7b546E38E414F7E9822514be443A4800529 then p1.price
         else p0.price
         end as paired_token_usd_price,     
    volume as trading_volume
from pool_liquidity l 
left join pools on l.pool = pools.address
left join tokens t0 on l.tokenA = t0.address
left join tokens t1 on l.tokenB = t1.address
left join tokens_prices_daily p0 on l.time = p0.time and l.tokenA = p0.token
left join tokens_prices_daily p1 on l.time = p1.time and l.tokenB = p1.token
left join trading_volume tv on l.time = tv.time and l.pool = tv.pool
) 


select blockchain||' '||project||' '||coalesce(paired_token_symbol,'unknown')||':'||main_token_symbol as pool_name, * 
from all_metrics

