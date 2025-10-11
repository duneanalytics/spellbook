{% macro pancakeswap_compatible_infinity_base_liquidity_events( 
    blockchain = null
    , project = 'pancakeswap'
    , version = 'infinity'
    , PoolManager_evt_ModifyLiquidity = null
    , PoolManager_evt_Swap = null
    , liquidity_pools = null
    , liquidity_sqrtpricex96 = null
    )
%}


with 

get_pools as (
    select 
        blockchain
        , id
        , token0
        , token1
    from 
    {{ liquidity_pools }}
),

get_prices_tmp as (
    select
        blockchain
        , id
        , block_index_sum 
        , previous_block_index_sum
        , sqrtpricex96
    from 
    {{ liquidity_sqrtpricex96 }}
),

get_latest_prices as (
    select 
        blockchain
        , id 
        , max(block_index_sum) as block_index_sum 
        , max(previous_block_index_sum) as previous_block_index_sum 
        , max_by(sqrtpricex96, block_index_sum) as sqrtpricex96
    from 
    get_prices_tmp 
    group by 1, 2 
),

get_prices as (
    select 
        blockchain
        , id
        , block_index_sum 
        , previous_block_index_sum
        , sqrtpricex96
    from 
    get_prices_tmp 

    union all 

    select 
        blockchain
        , id
        , block_index_sum + block_index_sum as block_index_sum 
        , block_index_sum as previous_block_index_sum -- for filling
        , sqrtpricex96
    from 
    get_latest_prices
),

modify_liquidity_events as (
    with 
    
    get_events as (
        select 
            evt_tx_from as tx_from
            , evt_block_time
            , evt_block_number
            , evt_block_number + evt_index/1e6 as block_index_sum
            , id
            , evt_tx_hash
            , evt_index
            , 'modify_liquidity' as event_type
            , salt
            , tickLower
            , tickUpper
            , liquidityDelta
            , sender -- needed for fee logic
        from 
        {{ PoolManager_evt_ModifyLiquidity }}
        {%- if is_incremental() %}
        where {{ incremental_predicate('evt_block_time') }}
        {%- endif %} 
    ),

    add_latest_price as (
        select 
            ab.*,
            gp.sqrtpricex96
        from (
        select 
            ge.*
            , gp.previous_block_index_sum
        from 
        get_events ge 
        left join 
        get_prices gp 
            on ge.id = gp.id 
            and ge.block_index_sum >= gp.previous_block_index_sum
            and ge.block_index_sum < gp.block_index_sum 
        ) ab 
        inner join 
        get_prices gp 
            on ab.id = gp.id
            and ab.previous_block_index_sum = gp.block_index_sum 
    ),

    prep_for_calculations as (
        select 
            * 
            , sqrtpricex96 / power(2, 96) AS sqrtprice
            , sqrt(power(1.0001, tickLower)) AS sqrtRatioL
            , sqrt(power(1.0001, tickUpper)) AS sqrtRatioU
        from 
        add_latest_price
    )

    select 
        *
        , case
            when sqrtPrice <= sqrtRatioL then liquidityDelta * ((sqrtRatioU - sqrtRatioL)/(sqrtRatioL*sqrtRatioU))
            when sqrtPrice >= sqrtRatioU then 0
            else liquidityDelta * ((sqrtRatioU - sqrtPrice) / (sqrtPrice * sqrtRatioU))
          end as amount0
        , case
            when sqrtPrice <= sqrtRatioL then 0
            when sqrtPrice >= sqrtRatioU then liquidityDelta * (sqrtRatioU - sqrtRatioL)
            else liquidityDelta * (sqrtPrice - sqrtRatioL)
          end as amount1
    from 
    prep_for_calculations
),

swap_events as (
    select 
        evt_tx_from as tx_from
        , evt_block_time
        , evt_block_number 
        , evt_tx_hash 
        , evt_index 
        , id 
        , -1 * amount0 as amount0
        , -1 * amount1 as amount1
    from 
    {{ PoolManager_evt_Swap }}
    {%- if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
),

liquidity_change_base as (
    select 
        ml.id
        , ml.tx_from 
        , ml.evt_block_time as block_time
        , ml.evt_block_number as block_number 
        , ml.evt_tx_hash as tx_hash 
        , ml.evt_index 
        , ml.event_type 
        , gp.token0 
        , gp.token1 
        , ml.amount0 
        , ml.amount1 
    from 
    modify_liquidity_events ml 
    inner join 
    get_pools gp 
        on ml.id = gp.id 

    union all 

    select 
        se.id
        , se.tx_from 
        , se.evt_block_time as block_time
        , se.evt_block_number as block_number 
        , se.evt_tx_hash as tx_hash 
        , se.evt_index 
        , 'swap' as event_type 
        , gp.token0 
        , gp.token1 
        , se.amount0 
        , se.amount1 
    from 
    swap_events se
    inner join 
    get_pools gp 
        on se.id = gp.id 
)
    select 
          '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , cast(date_trunc('month', block_time) as date) as block_month
        , cast(date_trunc('day', block_time) as date) as block_date
        , date_trunc('minute', block_time) as block_time -- for prices
        , block_number
        , id
        , tx_hash
        , tx_from
        , evt_index
        , event_type
        , token0
        , token1
        , CAST(amount0 AS double) as amount0_raw
        , CAST(amount1 AS double) as amount1_raw
    from 
    liquidity_change_base 

{% endmacro %}