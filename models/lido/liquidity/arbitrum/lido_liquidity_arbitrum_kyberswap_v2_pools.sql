{{ config(
    schema='lido_liquidity_arbitrum',
    alias = 'kyberswap_v2_pools',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe"]\') }}'
    )
}}

{% set project_start_date = '2023-05-01' %} 


with pools as (
select pool as address, 'arbitrum' as blockchain, 'kyberswap' as project, 
max(cast(swapFeeUnits as double))/1000 as fee,
token0, token1
from {{ source('kyber_arbitrum', 'ElasticFactoryV2_evt_PoolCreated') }}
where token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 or token1 = 0x5979D7b546E38E414F7E9822514be443A4800529
group by 1,2,3,5,6
)

, tokens as (
select distinct token as address--, pt.symbol, pt.decimals, tm.address_l1 
from (
select token1 as token
from {{ source('kyber_arbitrum', 'ElasticFactoryV2_evt_PoolCreated') }}
where token0 = 0x5979D7b546E38E414F7E9822514be443A4800529
union
select token0
from {{ source('kyber_arbitrum', 'ElasticFactoryV2_evt_PoolCreated') }}
where token1 = 0x5979D7b546E38E414F7E9822514be443A4800529 
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
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

      AND DATE_TRUNC('day', minute) < current_date
      AND blockchain = 'arbitrum'
      AND contract_address IN (SELECT address  FROM tokens where address NOT IN  (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 0x9cfb13e6c11054ac9fcb92ba89644f30775436e4 ))
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
        ORDER BY minute range BETWEEN UNBOUNDED preceding AND UNBOUNDED following) AS price
    FROM
      {{source('prices','usd')}}
    WHERE
      DATE_TRUNC('day', minute) = current_date
      AND blockchain = 'arbitrum'
      AND contract_address IN (SELECT address  FROM tokens where address NOT IN  (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 0x9cfb13e6c11054ac9fcb92ba89644f30775436e4 ))

  )

  
 , tokens_prices_hourly AS (
        select 
        time, 
        LEAD(time,1, now() + INTERVAL '1' hour) OVER (PARTITION BY token  ORDER BY time) AS next_time,
        token,
          decimals, 
          symbol,
        price from (
        SELECT DISTINCT
          DATE_TRUNC('hour', minute) AS time,
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
        {% endif %}
        {% if is_incremental() %}
        WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
        {% endif %}
         
          AND blockchain = 'arbitrum'
          AND contract_address IN (
            SELECT
              address
            FROM
              tokens
          )) p
      
  )
  

, swap_events as (
    select 
        date_trunc('day', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        sum(cast(deltaQty0 as DOUBLE)) as amount0,
        sum(cast(deltaQty1 as DOUBLE)) as amount1
        
    from {{ source('kyber_arbitrum', 'ElasticPoolV2_evt_Swap') }} sw
    left join {{ source('kyber_arbitrum', 'ElasticFactoryV2_evt_PoolCreated') }} cr on sw.contract_address = cr.pool
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
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
    from {{ source('kyber_arbitrum', 'ElasticPoolV2_evt_Mint') }} mt
    left join {{ source('kyber_arbitrum', 'ElasticFactoryV2_evt_PoolCreated') }} cr on mt.contract_address = cr.pool
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', mt.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', mt.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    and mt.contract_address  in (select address from pools)
    group by 1,2,3,4
    
)


, burn_events as (
    select 
        date_trunc('day', bn.evt_block_time) as time,
        bn.contract_address as pool,
        cr.token0, cr.token1,
        (-1)*sum(cast(qty0 as DOUBLE)) as amount0,
        (-1)*sum(cast(qty1 as DOUBLE)) as amount1
    from {{ source('kyber_arbitrum', 'ElasticPoolV2_evt_Burn') }} bn
    left join {{ source('kyber_arbitrum', 'ElasticFactoryV2_evt_PoolCreated') }} cr on bn.contract_address = cr.pool
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', bn.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', bn.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    and bn.contract_address  in (select address from pools)
    group by 1,2,3,4

    union all

    select 
        date_trunc('day', bn.evt_block_time), 
        bn.contract_address as pool,
        cr.token0, cr.token1,
        (-1) * sum(cast(qty0 as double)) as amount0, 
        (-1) * sum(cast(qty1 as double)) as amount1 
    from {{ source('kyber_arbitrum', 'ElasticPoolV2_evt_BurnRTokens') }} bn
    left join {{ source('kyber_arbitrum', 'ElasticFactoryV2_evt_PoolCreated') }} cr on bn.contract_address = cr.pool
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', bn.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', bn.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    and bn.contract_address  in (select address from pools)
    group by 1,2,3,4
)


    
, daily_delta_balance as (
    select time, pool, token0, token1, sum(coalesce(amount0, 0)) as amount0, sum(coalesce(amount1, 0)) as amount1,
        lead(time, 1, current_date + interval '1' day) over (partition by pool order by time) as next_time
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
        SELECT distinct
      time,
      pool,
      d.token0,
      d.token1,
      amount0,
      amount1
    FROM  daily_delta_balance d 

)


, swap_events_hourly as (
    select hour, pool, token0, token1, sum(amount0) as amount0, sum(amount1) as amount1 from (
    select 
        date_trunc('hour', sw.evt_block_time) as hour,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        coalesce(sum(cast(abs(deltaQty0) as DOUBLE)),0) as amount0,
        coalesce(sum(cast(abs(deltaQty1) as DOUBLE)),0) as amount1
        
    from {{source('kyber_arbitrum','ElasticPoolV2_evt_Swap')}} sw 
    left join {{source('kyber_arbitrum','ElasticFactoryV2_evt_PoolCreated')}} cr on sw.contract_address = cr.pool
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    and sw.contract_address in (select address from pools)
    group by 1,2,3,4
      ) a group by 1,2,3,4
) 

, trading_volume_hourly as (
    select hour as time, pool, token0, amount0, p.price, coalesce(p.price*amount0/power(10, p.decimals),0) as volume
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
 

  select pools.address as pool, pools.blockchain, pools.project, pools.fee, cast(l.time as date) as "time", 
    case when l.token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 then l.token0 else l.token1 end main_token,
    case when l.token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 then p0.symbol else p1.symbol end main_token_symbol,
    case when l.token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 then l.token1 else l.token0 end paired_token,
    case when l.token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 then p1.symbol else p0.symbol end paired_token_symbol, 
    case when l.token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 then amount0/power(10, p0.decimals)  else amount1/power(10, p1.decimals)  end main_token_reserve,
    case when l.token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 then amount1/power(10, p1.decimals)  else amount0/power(10, p0.decimals)  end paired_token_reserve,
    case when l.token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 then p0.price else p1.price end as main_token_usd_price,
    case when l.token0 = 0x5979D7b546E38E414F7E9822514be443A4800529 then p1.price else p0.price end as paired_token_usd_price,
    tv.volume as trading_volume 
  from pool_liquidity l 
  left join pools on l.pool = pools.address
  left join tokens t0 on l.token0 = t0.address
  left join tokens t1 on l.token1 = t1.address
  left join tokens_prices_daily p0 on l.time = p0.time and l.token0 = p0.token
  left join tokens_prices_daily p1 on l.time = p1.time and l.token1 = p1.token
  left join  trading_volume tv  on l.time = tv.time and l.pool = tv.pool
  
)
select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), coalesce(paired_token_symbol, 'unknown')),':') , main_token_symbol, ' ', format('%,.3f',round(coalesce(fee,0),4))) as pool_name,* 
from all_metrics

