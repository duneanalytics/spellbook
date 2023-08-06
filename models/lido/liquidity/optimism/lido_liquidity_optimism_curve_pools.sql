{{ config(
    schema='lido_liquidity_optimism',
    alias = alias('curve_pools'),
    tags = ['dunesql'], 
    partition_by = ['time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "lido_liquidity",
                                \'["ppclunghe", "kemasan"]\') }}'
    )
}}


{% set project_start_date = '2022-10-06' %}

with 
 weth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        symbol,
        decimals,
        avg(price) AS price
    FROM {{source('prices','usd')}} p
    
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

    and date_trunc('day', minute) < current_date
    and blockchain = 'optimism'
    and contract_address = 0x4200000000000000000000000000000000000006
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
    and blockchain = 'optimism'
    and contract_address = 0x4200000000000000000000000000000000000006

    
)    

, weth_prices_hourly AS (
    select time
    , lead(time,1, DATE_TRUNC('hour', now() + interval '1' hour)) over (order by time) as next_time
    , price
    from (
    SELECT distinct
        DATE_TRUNC('hour', minute) time
        , last_value(price) over (partition by DATE_TRUNC('hour', minute), contract_address ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM {{ source('prices', 'usd') }} p
    
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

    and blockchain = 'optimism'
    and contract_address = 0x4200000000000000000000000000000000000006
    
))   

, wsteth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        symbol,
        decimals,
        avg(price) AS price
    FROM {{source('prices','usd')}} p

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    
    and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'ethereum'
    and contract_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0
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
    and contract_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0

)

, add_liquidity_events as (
    select  date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , sum(cast(token_amounts[1] as double)) as eth_amount_raw
        , sum(cast(token_amounts[2] as double)) as wsteth_amount_raw
    from {{source('curvefi_optimism','wstETH_swap_evt_AddLiquidity')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    
    group by 1, 2
) 

, remove_liquidity_events as (
    select time, pool, sum(eth_amount_raw) as eth_amount_raw, sum(wsteth_amount_raw) as wsteth_amount_raw
    from (
    select date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , double '0' as eth_amount_raw
        , cast(coin_amount as double) as wsteth_amount_raw
    from {{source('curvefi_optimism','wstETH_swap_evt_RemoveLiquidityOne')}}
    --WHERE date_trunc('day', evt_block_time) >= date'{{ project_start_date }}'
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    
    and evt_tx_hash in (select evt_tx_hash from {{source('lido_optimism','wstETH_evt_Transfer')}})
    
    union all
    
    select date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , cast(coin_amount as double) as eth_amount_raw
        , double '0' as wsteth_amount_raw
    from {{source('curvefi_optimism','wstETH_swap_evt_RemoveLiquidityOne')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    
    and evt_tx_hash not in (select evt_tx_hash from {{source('lido_optimism','wstETH_evt_Transfer')}})
    
    union all
    
    select  date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , cast(token_amounts[1] as double)
        , cast(token_amounts[2] as double)
    from {{source('curvefi_optimism','wstETH_swap_evt_RemoveLiquidityImbalance')}}
    --WHERE date_trunc('day', evt_block_time) >= date'{{ project_start_date }}'
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    
    
    union all
    
    select date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , cast(token_amounts[1] as double)
        , cast(token_amounts[2] as double)
    from {{source('curvefi_optimism','wstETH_swap_evt_RemoveLiquidity')}}
    --WHERE date_trunc('day', evt_block_time) >= date'{{ project_start_date }}'
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    

) group by 1,2
)

, token_exchange_events as(
    select date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , sum(case when cast(sold_id as double) = double '0'
            then cast(tokens_sold as double) else (-1) * cast(tokens_bought as double) end) as eth_amount_raw
        , sum(case when cast(sold_id as double) = double '0'
            then (-1) * cast(tokens_bought as double) else cast(tokens_sold as double) end) as wsteth_amount_raw
    from {{source('curvefi_optimism','wstETH_swap_evt_TokenExchange')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}
    
    group by 1,2
)

, reserves as (
    select day
        , pool
        , main_token
        , main_token_symbol
        , paired_token
        , paired_token_symbol 
        , sum(main_token_reserve) as main_token_reserve
        , sum(main_token_usd_reserve) as main_token_usd_reserve
        , sum(paired_token_reserve) as paired_token_reserve
        , sum(paired_token_usd_reserve) as paired_token_usd_reserve

    from (
    select d.time as day
        , coalesce(d.pool, 0xb90b9b1f91a01ea22a182cd84c1e22222e39b415) as pool
        , case when p2.token = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 then 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb end as main_token
        , p2.symbol as main_token_symbol
        , p1.token as paired_token
        , p1.symbol as paired_token_symbol 
        , coalesce(d.wsteth_amount_raw, 0)/1e18 as main_token_reserve
        , coalesce(d.wsteth_amount_raw, 0)* p2.price/1e18 as main_token_usd_reserve
        , coalesce(d.eth_amount_raw, 0)/1e18 as paired_token_reserve
        , coalesce(d.eth_amount_raw, 0) * p1.price /1e18 as paired_token_usd_reserve
    from add_liquidity_events d
    left join weth_prices_daily p1 ON p1.time = d.time 
    left join wsteth_prices_daily p2 ON p2.time = d.time

    union all

    select w.time as day
        , coalesce(w.pool, 0xb90b9b1f91a01ea22a182cd84c1e22222e39b415) as pool
        , case when p2.token = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 then 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb end as main_token
        , p2.symbol as main_token_symbol
        , p1.token as paired_token
        , p1.symbol as paired_token_symbol 
        , -coalesce(w.wsteth_amount_raw, 0)/1e18 as main_token_reserve
        , -coalesce(w.wsteth_amount_raw, 0)* p2.price/1e18 as main_token_usd_reserve
        , -coalesce(w.eth_amount_raw, 0)/1e18 as paired_token_reserve
        , -coalesce(w.eth_amount_raw, 0) * p1.price /1e18 as paired_token_usd_reserve
    from remove_liquidity_events w
    left join weth_prices_daily p1 ON p1.time = w.time
    left join wsteth_prices_daily p2 ON p2.time = w.time

    union all

    select e.time as day
        , coalesce(e.pool, 0xb90b9b1f91a01ea22a182cd84c1e22222e39b415) as pool
        , case when p2.token = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 then 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb end as main_token
        , p2.symbol as main_token_symbol
        , p1.token as paired_token
        , p1.symbol as paired_token_symbol 
        , coalesce(e.wsteth_amount_raw, 0)/1e18 as main_token_reserve
        , coalesce(e.wsteth_amount_raw, 0)* p2.price/1e18 as main_token_usd_reserve
        , coalesce(e.eth_amount_raw, 0)/1e18 as paired_token_reserve
        , coalesce(e.eth_amount_raw, 0) * p1.price/1e18 as paired_token_usd_reserve
    from token_exchange_events e 
    left join weth_prices_daily p1 ON p1.time = e.time
    left join wsteth_prices_daily p2 ON p2.time = e.time
) group by 1,2,3,4,5,6
)

, token_exchange_hourly as( 
    select date_trunc('hour', evt_block_time) as time
        , sum(case when cast(sold_id as double) = 0 
            then cast(tokens_sold as double) else cast(tokens_bought as double) end) as eth_amount_raw
    from {{source('curvefi_optimism','wstETH_swap_evt_TokenExchange')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %} 

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


, all_metrics as (
    select pool 
        , blockchain
        , project
        , fee
        , time
        , main_token
        , main_token_symbol
        , paired_token
        , paired_token_symbol
        , sum(main_token_reserve) as main_token_reserve 
        , sum(paired_token_reserve) as paired_token_reserve
        , sum(main_token_usd_reserve) as main_token_usd_reserve
        , sum(paired_token_usd_reserve) as paired_token_usd_reserve
        , sum(trading_volume) as trading_volume 

    from (
    
    select  
        pool 
        , 'optimism' as blockchain
        , 'curve' as project
        , 0.04 as fee
        , cast(day as date) as time
        , main_token
        , main_token_symbol
        , paired_token
        , paired_token_symbol
        , main_token_reserve 
        , paired_token_reserve
        , main_token_usd_reserve
        , paired_token_usd_reserve
        , 0 as trading_volume 
    from reserves r
    
    union all

    select  
        0xb90b9b1f91a01ea22a182cd84c1e22222e39b415
        , 'optimism' as blockchain
        , 'curve' as project
        , 0.04 as fee
        , time
        , 0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb
        , 'wstETH'
        , 0x4200000000000000000000000000000000000006
        , 'WETH'
        , 0
        , 0
        , 0
        , 0
        , coalesce(volume,0) as trading_volume 
    from trading_volume
    )
    group by 1,2,3,4,5,6,7,8,9
    
)


select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), coalesce(paired_token_symbol,'unknown')),':') , main_token_symbol, ' ',  format('%,.3f%%',round(coalesce(fee,0),4))) as pool_name,* 
from all_metrics

