{{ config(
    schema='lido_liquidity_arbitrum',
    alias = alias('uniswap_v3_pools'),
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

{% set project_start_date = '2022-09-21' %} 

with dates as (
select explode(sequence(to_date('{{ project_start_date }}'), now(), interval 1 day)) as day
)

, pools as (
select pool as address, 'arbitrum' as blockchain, 'uniswap_v3' as project, fee/10000 as fee
from {{source('uniswap_v3_arbitrum','Factory_evt_PoolCreated')}}
where token0 = lower('0x5979D7b546E38E414F7E9822514be443A4800529') or token1 = lower('0x5979D7b546E38E414F7E9822514be443A4800529')
)

, pool_per_date as ( 
select dates.day, pools.*
from dates
left join pools on 1=1
)

, tokens_mapping as (
select lower(address_l1) as address_l1, lower(address_l2) as address_l2 from (values 
    ('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32', '0x13ad51ed4f1b7e9dc168d8a00cb3f4ddd85efa60'), --LDO
    ('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599', '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f'),   --WBTC
    ('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8'),   --USDC
    ('0xdAC17F958D2ee523a2206206994597C13D831ec7', '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'),   --USDT
    ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'),   --WETH
    ('0xB50721BCf8d664c30412Cfbc6cf7a15145234ad1', '0x912ce59144191c1204e64559fe8253a0e49e6548'),   --ARB
    ('0x514910771AF9Ca656af840dff83E8264EcF986CA', '0xf97f4df75117a78c1a5a0dbb814af92458539fb4'),  --LINK
    ('0x6B175474E89094C44Da98b954EedeAC495271d0F', '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1'), --DAI
    ('0xae78736Cd615f374D3085123A210448E74Fc6393', '0xec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8'), --rETH
    ('0xac3E018457B222d93114458476f3E3416Abbe38F', '0x95aB45875cFFdba1E5f451B950bC2E42c0053f39'), --sfrxETH
    ('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9', '0xba5ddd1f9d7f570dc94a51479a000e3bce967196'), --AAVE
    ('0x5f98805A4E8be255a32880FDeC7F6728C6568bA0', '0x93b346b6BC2548dA6A1E7d98E9a421B42541425b'), --LUSD
    ('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0', '0x5979d7b546e38e414f7e9822514be443a4800529') --wstETH
    
) as tokens(address_l1, address_l2)
)

, tokens as (
select distinct token as address, pt.symbol, pt.decimals, tm.address_l1 
from (
select token1 as token
from {{source('uniswap_v3_arbitrum','Factory_evt_PoolCreated')}}
where token0 = lower('0x5979D7b546E38E414F7E9822514be443A4800529') 
union
select token0
from {{source('uniswap_v3_arbitrum','Factory_evt_PoolCreated')}}
where token1 = lower('0x5979D7b546E38E414F7E9822514be443A4800529') 
union 
select lower('0x5979D7b546E38E414F7E9822514be443A4800529') 
) t
left join prices.tokens pt on ((t.token !=  lower('0x5979d7b546e38e414f7e9822514be443a4800529') and t.token = pt.contract_address) or
                               (t.token  =  lower('0x5979d7b546e38e414f7e9822514be443a4800529')  and pt.contract_address =  lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0')))
left join tokens_mapping tm on t.token = tm.address_l2                                    
)


, tokens_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        tokens_mapping.address_l2 as token,
        avg(price) AS price
    FROM {{ source('prices', 'usd') }}
    left join tokens_mapping on prices.usd.contract_address = tokens_mapping.address_l1
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address_l1 from tokens)
    group by 1,2
    union all
    SELECT distinct
        DATE_TRUNC('day', minute), 
        tokens_mapping.address_l2 as token,
        last_value(price) over (partition by DATE_TRUNC('day', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices', 'usd') }}
    left join tokens_mapping on prices.usd.contract_address = tokens_mapping.address_l1
    WHERE date_trunc('day', minute) = date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address_l1 from tokens)
)

, tokens_prices_hourly AS (
    select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1 hour')) over (partition by token order by time) as next_time, token, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time, 
        tokens_mapping.address_l2 as token,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices', 'usd') }}
    left join tokens_mapping on prices.usd.contract_address = tokens_mapping.address_l1
    WHERE date_trunc('day', minute) >= '{{ project_start_date }}' and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address in (select address_l1 from tokens)) p
)


, swap_events as (
    select 
        date_trunc('day', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.token0, cr.token1,
        sum(cast(amount0 as DOUBLE)) as amount0,
        sum(cast(amount1 as DOUBLE)) as amount1
        
    from {{source('uniswap_v3_arbitrum','Pair_evt_Swap')}} sw
    left join {{source('uniswap_v3_arbitrum','Factory_evt_PoolCreated')}} cr on sw.contract_address = cr.pool
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
    from {{source('uniswap_v3_arbitrum','Pair_evt_Mint')}} mt
    left join {{source('uniswap_v3_arbitrum','Factory_evt_PoolCreated')}} cr on mt.contract_address = cr.pool
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
    from {{source('uniswap_v3_arbitrum','Pair_evt_Collect')}} c
    left join uniswap_v3_arbitrum.Factory_evt_PoolCreated cr on c.contract_address = cr.pool
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
    from {{source('uniswap_v3_arbitrum','Pair_evt_Burn')}} bn
    left join {{source('uniswap_v3_arbitrum','Factory_evt_PoolCreated')}} cr on bn.contract_address = cr.pool
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
    select  c.day as time, c.address as pool, token0, token1, sum(amount0) over(partition by pool order by time) as amount0, sum(amount1)  over(partition by pool order by time) as amount1
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
        
    from {{source('uniswap_v3_arbitrum','Pair_evt_Swap')}} sw 
    left join {{source('uniswap_v3_arbitrum','Factory_evt_PoolCreated')}} cr on sw.contract_address = cr.pool
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
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then (case when amount0 > 0 then amount0/power(10, t0.decimals) else 0 end)  
          else (case when amount1 > 0 then amount1/power(10, t1.decimals) else 0 end)  
         end main_token_reserve,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then (case when amount1 > 0 then amount1/power(10, t1.decimals) else 0 end)  
          else (case when amount0 > 0 then  amount0/power(10, t0.decimals) else 0 end)
         end paired_token_reserve,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then (case when amount0 > 0 then p0.price*amount0/power(10, t0.decimals) else 0 end)
         else (case when amount1 > 0 then p1.price*amount1/power(10, t1.decimals) else 0 end)
         end as main_token_usd_reserve,
    case when token0 = LOWER('0x5979D7b546E38E414F7E9822514be443A4800529') then (case when amount1 > 0 then p1.price*amount1/power(10, t1.decimals) else 0 end)
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
