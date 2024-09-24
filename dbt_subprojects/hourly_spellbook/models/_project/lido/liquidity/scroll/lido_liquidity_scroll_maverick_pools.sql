{{ config(
    schema='lido_liquidity_scroll',
    alias = 'maverick_pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.time')],
    post_hook='{{ expose_spells(blockchains = \'["scroll"]\',
                                spell_type = "project",
                                spell_name = "lido_liquidity",
                                contributors = \'["pipistrella"]\') }}'
    )
}}

{% set project_start_date = '2024-07-29' %}

with

pools as (
select poolAddress, tokenA, tokenB, cast(feeAIn as double)/1e16 as fee
from {{source('maverick_v2_scroll','V2Factory_evt_PoolCreated')}}
where tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32
   or tokenB = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32
)


, tokens as (
select distinct token as address
from (
select tokenA as token
from {{source('maverick_v2_scroll','V2Factory_evt_PoolCreated')}}
where tokenB = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32
union
select tokenB
from {{source('maverick_v2_scroll','V2Factory_evt_PoolCreated')}}
where tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32
union
select 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32
) t
)

, tokens_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        symbol,
        decimals,
        avg(price) AS price
    FROM {{source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('p.minute') }}
    {% endif %}
    and date_trunc('day', minute) < current_date
    and blockchain = 'scroll'
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
    WHERE date_trunc('day', minute) = current_date
    and blockchain = 'scroll'
    and contract_address in (select address from tokens)
)

, wsteth_prices_hourly as (
   select time, lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time, price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time,
        last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{source('prices','usd')}} p
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('p.minute') }}
    {% endif %}
    and blockchain = 'scroll'
    and contract_address = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32
    ) p
)

, swap_events as (
    select
        date_trunc('day', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.tokenA, cr.tokenB,
        sum(case when json_extract_scalar(params, '$.tokenAIn') = 'true' then cast(amountIn as double) else (-1)*cast(amountOut as double) end) as amountA,
        sum(case when json_extract_scalar(params, '$.tokenAIn') = 'true' then (-1)*cast(amountOut as double) else cast(amountIn as double) end) as amountB
    from {{source('maverick_v2_scroll','V2Pool_evt_PoolSwap')}} sw
    left join {{source('maverick_v2_scroll','V2Factory_evt_PoolCreated')}} cr on sw.contract_address = cr.poolAddress
    join pools on sw.contract_address = pools.poolAddress
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('sw.evt_block_time') }}
    {% endif %}
    group by 1,2,3,4
)

, addliquidity_events as (
    select  date_trunc('day', a.evt_block_time) as time,
        a.contract_address as pool,
        cr.tokenA, cr.tokenB,
        sum(cast(tokenAAmount as double)) as  amountA,
        sum(cast(tokenBAmount as double)) as  amountB
from {{source('maverick_v2_scroll','V2Pool_evt_PoolAddLiquidity')}} a
left join {{source('maverick_v2_scroll','V2Factory_evt_PoolCreated')}} cr on a.contract_address = cr.poolAddress
join pools on a.contract_address = pools.poolAddress
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', a.evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE {{ incremental_predicate('a.call_block_time') }}
 {% endif %}
group by 1,2,3,4
)

, removeliquidity_events as (
select  date_trunc('day', a.evt_block_time) as time,
        a.contract_address as pool,
        cr.tokenA, cr.tokenB,
        (-1)*sum(cast(tokenAOut as double)) as amountA,
        (-1)*sum(cast(tokenBOut as double)) as  amountB
from {{source('maverick_v2_scroll','V2Pool_evt_PoolRemoveLiquidity')}} a
left join {{source('maverick_v2_scroll','V2Factory_evt_PoolCreated')}} cr on a.contract_address = cr.poolAddress
join pools on a.contract_address = pools.poolAddress
 {% if not is_incremental() %}
 WHERE DATE_TRUNC('day', a.evt_block_time) >= DATE '{{ project_start_date }}'
 {% else %}
 WHERE {{ incremental_predicate('a.call_block_time') }}
 {% endif %}
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


, pool_liquidity as (
SELECT  time, pools.fee,
        pool,
        b.tokenA,
        b.tokenB,
        coalesce((SUM(amountA)),0) AS amountA,
        coalesce((SUM(amountB)),0) AS amountB
FROM daily_delta_balance b
left join pools on b.pool = pools.poolAddress
GROUP BY 1,2,3,4,5
)



, wsteth_traded_hourly as (
 select
        date_trunc('hour', sw.evt_block_time) as time,
        sw.contract_address as pool,
        cr.tokenA, cr.tokenB,
        sum(case when (cr.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 and json_extract_scalar(params, '$.tokenAIn') = 'true') then amountIn
                 when (cr.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 and json_extract_scalar(params, '$.tokenAIn') = 'false') then amountOut
                 when (cr.tokenA != 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 and json_extract_scalar(params, '$.tokenAIn') = 'true') then amountOut
                 when (cr.tokenA != 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 and json_extract_scalar(params, '$.tokenAIn') = 'false') then amountIn
                 end) as amount
    from {{source('maverick_v2_scroll','V2Pool_evt_PoolSwap')}} sw
    left join {{source('maverick_v2_scroll','V2Factory_evt_PoolCreated')}} cr on sw.contract_address = cr.poolAddress
    join pools on sw.contract_address = pools.poolAddress
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', sw.evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    WHERE {{ incremental_predicate('sw.evt_block_time') }}
    {% endif %}
    
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
        'scroll' as blockchain,
        'maverick' as project,
        format('%,.3f',round(coalesce(fee,0),4)) as fee,
        cast(o.time as date) time,
        case when o.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 then o.tokenA else o.tokenB end as main_token,
        case when o.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 then pA.symbol else pB.symbol end as main_token_symbol,
        case when o.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 then o.tokenB else o.tokenA end as paired_token,
        case when o.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 then pB.symbol else pA.symbol end as paired_token_symbol,
        case when o.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 then amountA/power(10,pA.decimals) else amountB/power(10,pB.decimals) end as main_token_reserve,
        case when o.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 then amountB/power(10,pB.decimals) else amountA/power(10,pA.decimals) end as paired_token_reserve,
        case when o.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 then pA.price else pB.price end as main_token_usd_price,
        case when o.tokenA = 0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32 then pB.price else pA.price end as paired_token_usd_price,
        coalesce(t.volume,0) as trading_volume
from pool_liquidity o
left join tokens_prices_daily pA on o.time = pA.time and o.tokenA = pA.token
left join tokens_prices_daily pB on o.time = pB.time and o.tokenB = pB.token
left join trading_volume t on o.time = t.time and o.pool = t.pool
)


select  blockchain||' '||project||' '||coalesce(paired_token_symbol,'unknown')||':'||main_token_symbol||' '||fee|| '('||cast(pool as varchar)||')' as pool_name,
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
        main_token_usd_price,
        paired_token_usd_price,
        trading_volume
from all_metrics

