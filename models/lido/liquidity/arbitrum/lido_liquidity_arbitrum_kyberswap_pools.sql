{{ config(
	tags=['legacy'],
	
    schema='lido_liquidity_arbitrum',
    alias = alias('kyberswap_pools', legacy_model=True),
    partition_by = ['time'],
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe", "gregshestakovlido"]\') }}'
    )
}}

{% set project_start_date = '2022-02-11' %} 

with dates as (
select explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 day)) as day
)
 
, pools as (
select pool as address, 'arbitrum' as blockchain, 'kyberswap' as project, swapFeeUnits/1000 as fee
from {{ source('kyber_arbitrum', 'Elastic_Factory_evt_PoolCreated') }}
where token0 = lower('0x5979D7b546E38E414F7E9822514be443A4800529') or token1 = lower('0x5979D7b546E38E414F7E9822514be443A4800529')

)

, tokens_mapping as (
select lower(address_l1) as address_l1, lower(address_l2) as address_l2 from
  (values 
    ('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32', '0x13ad51ed4f1b7e9dc168d8a00cb3f4ddd85efa60'), --LDO
    ('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599', '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f'),   --WBTC
    ('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8'),   --USDC
    ('0xdeFA4e8a7bcBA345F687a2f1456F5Edd9CE97202', '0xe4dddfe67e7164b0fe14e218d80dc4c08edc01cb'), -- KNC
    ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'),   --WETH
    ('0xB50721BCf8d664c30412Cfbc6cf7a15145234ad1', '0x912ce59144191c1204e64559fe8253a0e49e6548'),   --ARB
    ('0x514910771AF9Ca656af840dff83E8264EcF986CA', '0xf97f4df75117a78c1a5a0dbb814af92458539fb4'),  --LINK
    ('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0', '0x5979d7b546e38e414f7e9822514be443a4800529') --wstETH
) as tokens(address_l1, address_l2)
)

, tokens as (
select distinct token as address, pt.symbol, pt.decimals, tm.address_l1 
from (
select token1 as token
from {{ source('kyber_arbitrum', 'Elastic_Factory_evt_PoolCreated') }}
where token0 = lower('0x5979D7b546E38E414F7E9822514be443A4800529') 
union
select token0
from {{ source('kyber_arbitrum', 'Elastic_Factory_evt_PoolCreated') }}
where token1 = lower('0x5979D7b546E38E414F7E9822514be443A4800529') 
union 
select lower('0x5979D7b546E38E414F7E9822514be443A4800529') 
) t
left join {{ref('prices_tokens_legacy')}} pt on ((t.token !=  lower('0x5979d7b546e38e414f7e9822514be443a4800529') and t.token = pt.contract_address) or
                               (t.token  =  lower('0x5979d7b546e38e414f7e9822514be443a4800529')  and pt.contract_address =  lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0')))
left join tokens_mapping tm on t.token = tm.address_l2                                    
)


, tokens_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        tokens_mapping.address_l2 as token,
        avg(price) AS price
    FROM {{ source('prices', 'usd') }} p
    left join tokens_mapping on p.contract_address = tokens_mapping.address_l1
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address_l1 from tokens)
    group by 1,2
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        tokens_mapping.address_l2 as token,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices', 'usd') }} p
    left join tokens_mapping on p.contract_address = tokens_mapping.address_l1
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address_l1 from tokens)
)

, tokens_prices_hourly AS (
    select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1 hour')) over (partition by token order by time) as next_time, token, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) as time, 
        tokens_mapping.address_l2 as token,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices', 'usd') }} p
    left join tokens_mapping on p.contract_address = tokens_mapping.address_l1
    WHERE date_trunc('hour', minute) >= '{{ project_start_date }}'
    and blockchain = 'ethereum'
    and contract_address in (select address_l1 from tokens)
    
) p
)


, swap_events as (
    select 
        date_trunc('day', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        sum(cast(deltaQty0 as DOUBLE)) as amount0,
        sum(cast(deltaQty1 as DOUBLE)) as amount1
        
    from {{ source('kyber_arbitrum', 'Elastic_Pool_evt_swap') }} sw
    left join {{ source('kyber_arbitrum', 'Elastic_Factory_evt_PoolCreated') }} cr on sw.contract_address = cr.pool
    WHERE date_trunc('day', sw.evt_block_time) >= '{{ project_start_date }}'
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
    from {{ source('kyber_arbitrum', 'Elastic_Pool_evt_Mint') }} mt
    left join {{ source('kyber_arbitrum', 'Elastic_Factory_evt_PoolCreated') }} cr on mt.contract_address = cr.pool
    WHERE date_trunc('day', mt.evt_block_time) >= '{{ project_start_date }}'
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
    from {{ source('kyber_arbitrum', 'Elastic_Pool_evt_Burn') }} bn
    left join {{ source('kyber_arbitrum', 'Elastic_Factory_evt_PoolCreated') }} cr on bn.contract_address = cr.pool
    WHERE date_trunc('day', bn.evt_block_time) >= '{{ project_start_date }}'
    and bn.contract_address  in (select address from pools)
    group by 1,2,3,4

    union all

    select 
        date_trunc('day', bn.evt_block_time), 
        bn.contract_address as pool,
        cr.token0, cr.token1,
        (-1) * sum(cast(qty0 as double)) as amount0, 
        (-1) * sum(cast(qty1 as double)) as amount1 
    from {{ source('kyber_arbitrum', 'Elastic_Pool_evt_BurnRTokens') }} bn
    left join {{ source('kyber_arbitrum', 'Elastic_Factory_evt_PoolCreated') }} cr on bn.contract_address = cr.pool
    WHERE date_trunc('day', bn.evt_block_time) >= '{{ project_start_date }}'
    and bn.contract_address  in (select address from pools)
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
    ) balance
    group by 1,2,3,4
)
  
, pool_liquidity as (
    select time, pool, token0, token1, sum(amount0) over(partition by pool order by time) as amount0, 
    sum(amount1)  over(partition by pool order by time) as amount1
    from daily_delta_balance b 

)


, swap_events_hourly as (
    select hour, pool, token0, token1, sum(amount0) as amount0, sum(amount1) as amount1 from (
    select 
        date_trunc('hour', sw.evt_block_time) as hour,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        coalesce(sum(cast(abs(deltaQty0) as DOUBLE)),0) as amount0,
        coalesce(sum(cast(abs(deltaQty1) as DOUBLE)),0) as amount1
        
    from {{source('kyber_arbitrum','Elastic_Pool_evt_swap')}} sw 
    left join {{source('kyber_arbitrum','Elastic_Factory_evt_PoolCreated')}} cr on sw.contract_address = cr.pool
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
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then token0 else token1 end main_token,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then t0.symbol else t1.symbol end main_token_symbol,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then token1 else token0 end paired_token,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then t1.symbol else t0.symbol end paired_token_symbol, 
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then amount0/power(10, t0.decimals)  else amount1/power(10, t1.decimals)  end main_token_reserve,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then amount1/power(10, t1.decimals)  else amount0/power(10, t0.decimals)  end paired_token_reserve,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then p0.price*amount0/power(10, t0.decimals) else p1.price*amount1/power(10, t1.decimals) end as main_token_usd_reserve,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then p1.price*amount1/power(10, t1.decimals) else p0.price*amount0/power(10, t0.decimals) end as paired_token_usd_reserve,
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
where main_token_reserve > 1