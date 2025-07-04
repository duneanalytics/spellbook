{{ config(
    schema = 'uniswap'
    , alias = 'tvl_daily'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'id', 'block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , post_hook='{{ expose_spells(\'[
                                      "ethereum","arbitrum","base","ink","blast","optimism","blast","bnb","zora","avalanche_c","unichain","worldchain"
                                    ]\',
                                    "project",
                                    "uniswap",
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
        , token0_balance_raw as amount0_raw 
        , token1_balance_raw as amount1_raw 
        , token0_balance as amount0 
        , token1_balance as amount1
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

days as (
    select
        timestamp as day 
    from 
    {{ source('utils','days') }}
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
),

prices as (
    select
        cast(date_trunc('day', minute) as date) as block_date
        , blockchain
        , contract_address
        , max_by(price, minute) as price
    from 
    {{ source('prices','usd_with_native') }}
    where {{ incremental_predicate('minute') }}
    group by 1, 2, 3 
),

prices_day as (
    select
        cast(date_trunc('day', timestamp) as date) as block_date
        , blockchain
        , contract_address
        , price
    from 
    {{ source('prices','day') }}
    where volume is not null 
    and volume > 500000 -- greater than $500k day volume 
    and {{ incremental_predicate('timestamp') }}
)

    select 
        block_month
        , tl.block_date
        , tl.blockchain
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
        , token0_balance * coalesce(pa.price, pd_a.price) as token0_balance_usd
        , token1_balance * coalesce(pb.price, pd_b.price) as token1_balance_usd
    from 
    tvl_daily tl 
    left join 
    prices pa 
        on tl.token0 = pa.contract_address 
        and tl.block_date = pa.block_date 
        and tl.blockchain = pa.blockchain 
    left join 
    prices pb
        on tl.token1 = pb.contract_address 
        and tl.block_date = pb.block_date 
        and tl.blockchain = pb.blockchain
    left join 
    prices_day pd_a
        on tl.token0 = pd_a.contract_address 
        and tl.block_date = pd_a.block_date 
        and tl.blockchain = pd_a.blockchain 
    left join 
    prices_day pd_b
        on tl.token1 = pd_b.contract_address 
        and tl.block_date = pd_b.block_date 
        and tl.blockchain = pd_b.blockchain 
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

days as (
    select
        timestamp as day 
    from 
    {{ source('utils','days') }}
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
),

prices as (
    select
        cast(date_trunc('day', minute) as date) as block_date
        , blockchain
        , contract_address
        , max_by(price, minute) as price
    from 
    {{ source('prices','usd_with_native') }}
    group by 1, 2, 3 
),

prices_day as (
    select
        cast(date_trunc('day', timestamp) as date) as block_date
        , blockchain
        , contract_address
        , price
    from 
    {{ source('prices','day') }}
    where volume is not null 
    and volume > 500000 -- greater than $500k day volume 
)

    select 
        block_month
        , tl.block_date
        , tl.blockchain
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
        , token0_balance * coalesce(pa.price, pd_a.price) as token0_balance_usd
        , token1_balance * coalesce(pb.price, pd_b.price) as token1_balance_usd
    from 
    tvl_daily tl 
    left join 
    prices pa 
        on tl.token0 = pa.contract_address 
        and tl.block_date = pa.block_date 
        and tl.blockchain = pa.blockchain 
    left join 
    prices pb
        on tl.token1 = pb.contract_address 
        and tl.block_date = pb.block_date 
        and tl.blockchain = pb.blockchain 
    left join 
    prices_day pd_a
        on tl.token0 = pd_a.contract_address 
        and tl.block_date = pd_a.block_date 
        and tl.blockchain = pd_a.blockchain 
    left join 
    prices_day pd_b
        on tl.token1 = pd_b.contract_address 
        and tl.block_date = pd_b.block_date 
        and tl.blockchain = pd_b.blockchain 
{% endif %}