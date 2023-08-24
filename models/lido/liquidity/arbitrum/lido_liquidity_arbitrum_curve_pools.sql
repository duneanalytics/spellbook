{{ config(
    schema='lido_liquidity_arbitrum',
    alias = alias('curve_pools'),
    tags = ['dunesql'], 
    partition_by = ['time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'time'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
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
    --WHERE date_trunc('day', minute) >= date '{{ project_start_date }}' 
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

    and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'arbitrum'
    and contract_address = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
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
    and blockchain = 'arbitrum'
    and contract_address = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1

    
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
    --WHERE date_trunc('hour', minute) >= date '{{ project_start_date }}' 
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

    and blockchain = 'arbitrum'
    and contract_address = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
    
))   

, wsteth_prices_daily AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        contract_address as token,
        symbol,
        decimals,
        avg(price) AS price
    FROM {{source('prices','usd')}} p
    --WHERE date_trunc('day', minute) >= date'{{ project_start_date }}' 

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', p.minute) >= DATE '{{ project_start_date }}'
    {% else %}
    
    WHERE DATE_TRUNC('day', p.minute) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

    and date_trunc('day', minute) < date_trunc('day', now())
    and blockchain = 'arbitrum'
    and contract_address = 0x5979d7b546e38e414f7e9822514be443a4800529
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
    and blockchain = 'arbitrum'
    and contract_address = 0x5979d7b546e38e414f7e9822514be443a4800529

)

, add_liquidity_events as (
    select  date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , sum(token_amounts[1]) as eth_amount_raw
        , sum(token_amounts[2]) as wsteth_amount_raw
    from {{source('curvefi_arbitrum','wstETH_swap_evt_AddLiquidity')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'
    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

    group by 1, 2
) 

, remove_liquidity_events as (
    select time, pool, sum(eth_amount_raw) as eth_amount_raw, sum(wsteth_amount_raw) as wsteth_amount_raw
    from (
    select date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , uint256 '0' as eth_amount_raw
        , coin_amount as wsteth_amount_raw
    from {{source('curvefi_arbitrum','wstETH_swap_evt_RemoveLiquidityOne')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

    and evt_tx_hash in (select evt_tx_hash from {{source('lido_arbitrum','wstETH_evt_Transfer')}})
    
    union all
    
    select date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , coin_amount as eth_amount_raw
        , uint256 '0' as wsteth_amount_raw
    from {{source('curvefi_arbitrum','wstETH_swap_evt_RemoveLiquidityOne')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

    and evt_tx_hash not in (select evt_tx_hash from {{source('lido_arbitrum','wstETH_evt_Transfer')}})
    
    union all
    
    select  date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , token_amounts[1]
        , token_amounts[2]
    from {{source('curvefi_arbitrum','wstETH_swap_evt_RemoveLiquidityImbalance')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

           

    union all
    
    select date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , token_amounts[1]
        , token_amounts[2]
    from {{source('curvefi_arbitrum','wstETH_swap_evt_RemoveLiquidity')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE_TRUNC('day', NOW() - INTERVAL '1' day)
    {% endif %}

        

) group by 1,2
)

, token_exchange_events as(
    select date_trunc('day', evt_block_time) as time
        , contract_address as pool
        , sum(case when sold_id = int256 '0' then cast(tokens_sold as double) else (-1)*cast(tokens_bought as double) end) as eth_amount_raw
        , sum(case when sold_id = int256 '0' then (-1)*cast(tokens_bought as double) else cast(tokens_sold as double) end) as wsteth_amount_raw
    from {{source('curvefi_arbitrum','wstETH_swap_evt_TokenExchange')}}
    --WHERE date_trunc('day', evt_block_time) >= date '{{ project_start_date }}'

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
    
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
        , sum(paired_token_reserve) as paired_token_reserve
    
    from (
    select d.time as day
        , coalesce(d.pool, 0x6eb2dc694eb516b16dc9fbc678c60052bbdd7d80) as pool
        , p2.token as main_token
        , p2.symbol as main_token_symbol
        , p1.token as paired_token
        , p1.symbol as paired_token_symbol 
        , coalesce(cast(d.wsteth_amount_raw as double), 0)/1e18 as main_token_reserve
        , coalesce(cast(d.eth_amount_raw as double), 0)/1e18 as paired_token_reserve
    
    from add_liquidity_events d
    left join weth_prices_daily p1 ON p1.time = d.time 
    left join wsteth_prices_daily p2 ON p2.time = d.time

    union all

    select w.time as day
        , coalesce(w.pool, 0x6eb2dc694eb516b16dc9fbc678c60052bbdd7d80) as pool
        , p2.token as main_token
        , p2.symbol as main_token_symbol
        , p1.token as paired_token
        , p1.symbol as paired_token_symbol 
        , (-1)*coalesce(cast(w.wsteth_amount_raw as double), 0)/1e18 as main_token_reserve
        , (-1)*coalesce(cast(w.eth_amount_raw as double), 0)/1e18 as paired_token_reserve
        
    from remove_liquidity_events w
    left join weth_prices_daily p1 ON p1.time = w.time
    left join wsteth_prices_daily p2 ON p2.time = w.time

    union all

    select e.time as day
        , coalesce(e.pool, 0x6eb2dc694eb516b16dc9fbc678c60052bbdd7d80) as pool
        , p2.token as main_token
        , p2.symbol as main_token_symbol
        , p1.token as paired_token
        , p1.symbol as paired_token_symbol 
        , coalesce(cast(e.wsteth_amount_raw as double), 0)/1e18 as main_token_reserve
        , coalesce(cast(e.eth_amount_raw as double), 0)/1e18 as paired_token_reserve
        
    from token_exchange_events e 
    left join weth_prices_daily p1 ON p1.time = e.time
    left join wsteth_prices_daily p2 ON p2.time = e.time
) group by 1,2,3,4,5,6
)


, token_exchange_hourly as( 
    select date_trunc('hour', evt_block_time) as time
        , sum(case when sold_id = int256 '0' then tokens_sold else tokens_bought end) as eth_amount_raw
    from {{source('curvefi_arbitrum','wstETH_swap_evt_TokenExchange')}}

    {% if not is_incremental() %}
    WHERE DATE_TRUNC('day', evt_block_time) >= DATE '{{ project_start_date }}'
    {% else %}
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
       
    select  
        pool 
        , 'arbitrum' as blockchain
        , 'curve' as project
        , 0.04 as fee
        , cast(day as date) as time
        , main_token
        , main_token_symbol
        , paired_token
        , paired_token_symbol
        , main_token_reserve 
        , paired_token_reserve
        , p2.price as main_token_usd_price
        , p1.price as paired_token_usd_price
        , coalesce(volume,0) as trading_volume
    from reserves r
    left join weth_prices_daily p1 ON p1.time = r.day 
    left join wsteth_prices_daily p2 ON p2.time = r.day
    left join trading_volume on  r.day = trading_volume.time
    
)    


select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(blockchain,CONCAT(' ', project)) ,' '), coalesce(paired_token_symbol,'unknown')),':') , main_token_symbol, ' ',  format('%,.3f%%',round(coalesce(fee,0),4))) as pool_name,* 
from all_metrics

