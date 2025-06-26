{{ config(
    schema = 'uniswap'
    , alias = 'tvl_daily'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'id', 'block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'[
                                      "ethereum","arbitrum","base","ink","blast","optimism","blast","bnb","zora","avalanche_c","unichain","worldchain"
                                    ]\',
                                    "sector",
                                    "dex",
                                    \'["Henrystats"]\') }}')
}}

{% if is_incremental() %}

with

daily_events as (
    select 
        block_month
        , block_date
        , blockchain
        , project
        , version
        , id
        , token0 
        , token1 
        , token0_symbol 
        , token1_symbol 
        , amount0_raw 
        , amount1_raw 
        , amount0 
        , amount1
    from 
    {{ ref('uniswap_daily_agg_liquidity_events') }}
    where {{ incremental_predicate('block_date') }}
),

min_daily as (
    select 
        min(block_date) as block_date 
    from 
    daily_events
),

tvl_min_daily as (
    select 
        block_month
        , block_date
        , blockchain
        , project
        , version
        , id
        , token0 
        , token1 
        , token0_symbol 
        , token1_symbol 
        , amount0_raw 
        , amount1_raw 
        , amount0 
        , amount1
    from 
    {{this}}
    where block_date = (select block_date from min_daily)
),

daily_events_final as (
    select 
        *
        , 'include' as check_filter
    from 
    daily_events 
    where block_date != (select block_date from min_daily)

    union all 

    select 
        *
        , 'exclude' as check_filter
    from 
    tvl_min_daily 
),

daily_cum as (
    select 
        * 
        , sum(amount0_raw) over (partition by blockchain, project, version, id order by block_date asc) as token0_balance_raw
        , sum(amount1_raw) over (partition by blockchain, project, version, id order by block_date asc) as token1_balance_raw
        , sum(amount0) over (partition by blockchain, project, version, id order by block_date asc) as token0_balance
        , sum(amount1) over (partition by blockchain, project, version, id order by block_date asc) as token1_balance
        , lead(block_date, 1, current_timestamp) over (partition by blockchain, project, version, id order by block_date asc) as next_day
    from 
    daily_events_final 
),

time_seq as (
    select
        sequence(
        cast('2025-01-01' as timestamp),
        date_trunc('day', cast(now() as timestamp)),
        interval '1' day
        ) as time 
),

days as (
    select
        time.time AS day 
    from 
    time_seq
    cross join unnest(time) AS time(time)
),

tvl_daily as (
    select 
        cast(date_trunc('month', d.day) as date) as block_month
        , cast(date_trunc('day', d.day) as date) as block_date
        , blockchain
        , project
        , version
        , id
        , token0 
        , token1 
        , token0_symbol 
        , token1_symbol 
        , token0_balance_raw 
        , token1_balance_raw 
        , token0_balance
        , token1_balance
        , check_filter 
    from 
    daily_cum c
    inner join 
    days d 
        on c.block_date <= d.day 
        and d.day < c.next_day
)

    select 
        block_month
        , block_date
        , blockchain
        , project
        , version
        , id
        , token0 
        , token1 
        , token0_symbol 
        , token1_symbol 
        , token0_balance_raw 
        , token1_balance_raw 
        , token0_balance
        , token1_balance
    from 
    tvl_daily 
    where check_filter = 'include'  

{% else %}

with

daily_events as (
    select 
        block_month
        , block_date
        , blockchain
        , project
        , version
        , id
        , token0 
        , token1 
        , token0_symbol 
        , token1_symbol 
        , amount0_raw 
        , amount1_raw 
        , amount0 
        , amount1
    from 
    {{ ref('uniswap_daily_agg_liquidity_events') }}
),

daily_cum as (
    select 
        * 
        , sum(amount0_raw) over (partition by blockchain, project, version, id order by block_date asc) as token0_balance_raw
        , sum(amount1_raw) over (partition by blockchain, project, version, id order by block_date asc) as token1_balance_raw
        , sum(amount0) over (partition by blockchain, project, version, id order by block_date asc) as token0_balance
        , sum(amount1) over (partition by blockchain, project, version, id order by block_date asc) as token1_balance
        , lead(block_date, 1, current_timestamp) over (partition by blockchain, project, version, id order by block_date asc) as next_day
    from 
    daily_events
),

time_seq as (
    select
        sequence(
        cast('2025-01-01' as timestamp),
        date_trunc('day', cast(now() as timestamp)),
        interval '1' day
        ) as time 
),

days as (
    select
        time.time AS day 
    from 
    time_seq
    cross join unnest(time) AS time(time)
),

tvl_daily as (
    select 
        cast(date_trunc('month', d.day) as date) as block_month
        , cast(date_trunc('day', d.day) as date) as block_date
        , blockchain
        , project
        , version
        , id
        , token0 
        , token1 
        , token0_symbol 
        , token1_symbol 
        , token0_balance_raw 
        , token1_balance_raw 
        , token0_balance
        , token1_balance
    from 
    daily_cum c
    inner join 
    days d 
        on c.block_date <= d.day 
        and d.day < c.next_day
)

    select 
        block_month
        , block_date
        , blockchain
        , project
        , version
        , id
        , token0 
        , token1 
        , token0_symbol 
        , token1_symbol 
        , token0_balance_raw 
        , token1_balance_raw 
        , token0_balance
        , token1_balance
    from 
    tvl_daily 

{% endif %}