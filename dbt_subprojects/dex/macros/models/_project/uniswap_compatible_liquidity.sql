{% macro uniswap_compatible_v4_liquidity_pools(
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_Initialize = null
    )
%}

    select 
        '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , contract_address
        , evt_block_time as creation_block_time
        , evt_block_number as creation_block_number
        , id
        , evt_tx_hash as tx_hash
        , evt_index
        , currency0 as token0
        , currency1 as token1
    from 
    {{ PoolManager_evt_Initialize }}
    {%- if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {%- endif %}

{% endmacro %}

{% macro uniswap_compatible_v4_liquidity_sqrtpricex96(
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_Initialize = null
    , PoolManager_evt_Swap = null 
    )
%}

{% if is_incremental() %}

with

base_events as (
    select 
        id
        , 'include' as check_filter
        , evt_block_time as block_time
        , evt_block_number as block_number
        , evt_index
        , evt_block_number + evt_index/1e6 as block_index_sum
        , sqrtpricex96
    from 
    {{ PoolManager_evt_Initialize }}
    where sqrtPriceX96 is not null 
    and {{ incremental_predicate('evt_block_time') }}

    union all 

    select 
        id
        , 'include' as check_filter
        , evt_block_time as block_time
        , evt_block_number as block_number
        , evt_index
        , evt_block_number + evt_index/1e6 as block_index_sum 
        , sqrtpricex96
    from 
    {{ PoolManager_evt_Swap }}
    where sqrtPriceX96 is not null 
    and {{ incremental_predicate('evt_block_time') }}
),

get_active_pools as ( -- get only the pools that were active on incremental run
    select 
       distinct id
    from 
    base_events 
),

get_latest_active_pools as (
    select 
        th.id
        , 'exclude' as check_filter
        , max_by(th.block_time, th.block_index_sum) as block_time
        , max_by(th.block_number, th.block_index_sum) as block_number
        , max_by(th.evt_index, th.block_index_sum) as evt_index
        , max(block_index_sum) as block_index_sum 
        , max_by(sqrtpricex96, th.block_index_sum) as sqrtpricex96
    from 
    {{this}} th 
    inner join 
    get_active_pools ga 
        on th.id = ga.id 
    group by 1

    union all 

    select 
        id
        , check_filter
        , block_time
        , block_number
        , evt_index
        , block_index_sum 
        , sqrtpricex96
    from 
    base_events
), 

sort_table as (
    select 
        *
        , lag(block_index_sum, 1, 0) over (partition by id order by block_index_sum) as previous_block_index_sum
    from 
    get_latest_active_pools
)

    select 
        '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , id
        , block_time
        , block_number
        , evt_index 
        , block_index_sum 
        , previous_block_index_sum
        , sqrtpricex96
    from 
    sort_table 
    where check_filter = 'include'


{% else %}



    with 

    get_events as (
        select 
            id
            , evt_block_time as block_time
            , evt_block_number as block_number
            , evt_index 
            , evt_block_number + evt_index/1e6 as block_index_sum 
            , sqrtpricex96
        from 
        {{ PoolManager_evt_Initialize }}
        where sqrtPriceX96 is not null 

        union all 

        select 
            id
            , evt_block_time as block_time
            , evt_block_number as block_number
            , evt_index 
            , evt_block_number + evt_index/1e6 as block_index_sum
            , sqrtpricex96 
        from 
        {{ PoolManager_evt_Swap }}
        where sqrtPriceX96 is not null 
    )
    

    select 
        '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , id
        , block_time
        , block_number
        , evt_index 
        , block_index_sum 
        , lag(block_index_sum, 1, 0) over (partition by id order by block_index_sum) as previous_block_index_sum
        , sqrtpricex96
    from 
    get_events 

{% endif %}

{% endmacro %}

{% macro uniswap_compatible_v4_base_liquidity_events( 
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_ModifyLiquidity = null
    , PoolManager_evt_Swap = null
    , PoolManager_call_Take = null 
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
              evt_block_time
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

fee_collection as (
    with 

    get_fees as (
        select 
            call_tx_hash as tx_hash 
            , call_block_time as block_time 
            , call_block_number as block_number 
            , 1 as evt_index -- we don't actually use index here for anything and some indexes are missing in the call table so hardcoding here as evt_index 
            , amount 
            , currency 
        from 
        {{ PoolManager_call_Take }}
        where call_success
        {%- if is_incremental() %}
        and {{ incremental_predicate('call_block_time') }}
        {%- endif %} 
    ),

    agg_fees as (
        select 
            tx_hash
            , block_time
            , block_number
            , min(evt_index) as evt_index
            , sum(amount) as fee_amount 
            , currency as fee_currency
        from 
        get_fees 
        group by 1, 2, 3, 6 
    ),

    modify_events as (
        select 
            evt_block_time as block_time
            , evt_block_number as block_number 
            , id 
            , evt_tx_hash as tx_hash 
            , evt_index 
            , amount0 
            , amount1 
        from 
        modify_liquidity_events
        where liquidityDelta <= int256 '0'
    ),

    single_pools as (
        select 
            tx_hash 
            , count(distinct id) as num_pools 
        from 
        modify_events 
        group by 1 
        having count(distinct id) = 1 -- only one pool in txn 
    ),

    agg_events as (
        select 
            me.tx_hash
            , me.block_number 
            , me.id 
            , sum(me.amount0) as amount0
            , sum(me.amount1) as amount1 
        from 
        modify_events me 
        inner join 
        single_pools sp 
            on me.tx_hash = sp.tx_hash 
        group by 1, 2, 3 
    ),

    join_with_pools as (
        select 
            gf.tx_hash 
            , gf.block_time 
            , gf.block_number 
            , gf.evt_index 
            , 'fee_collection' as event_type
            , ae.id 
            , -amount0 as modify_amount0 
            , -amount1 as modify_amount1
            , token0 
            , token1 
            , sum(case when fee_currency = token0 then fee_amount else 0 end) as amount0 
            , sum(case when fee_currency = token1 then fee_amount else 0 end) as amount1
        from 
        agg_fees gf 
        inner join 
        agg_events ae 
            on gf.tx_hash = ae.tx_hash 
            and gf.block_number = ae.block_number 
        inner join 
        get_pools gp 
            on ae.id = gp.id 
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )

    select 
        id
        , block_time 
        , block_number 
        , tx_hash 
        , evt_index
        , event_type 
        , token0 
        , token1 
        , case 
            when amount0 > modify_amount0 then amount0 - modify_amount0 else 0 
        end as amount0 -- subtract total modify liquidity amount from total amount logged in take()
        , case 
            when amount1 > modify_amount1 then amount1 - modify_amount1 else 0 
        end as amount1
    from 
    join_with_pools
),

swap_events as (
    select 
        evt_block_time
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

    union all 

    select 
        id
        , block_time
        , block_number 
        , tx_hash 
        , evt_index 
        , event_type 
        , token0 
        , token1 
        , -amount0 as amount0
        , -amount1 as amount1
    from 
    fee_collection
    where tx_hash not in (select evt_tx_hash from swap_events)
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
        , evt_index
        , event_type
        , token0
        , token1
        , CAST(amount0 AS double) as amount0_raw
        , CAST(amount1 AS double) as amount1_raw
    from 
    liquidity_change_base 

{% endmacro %}
