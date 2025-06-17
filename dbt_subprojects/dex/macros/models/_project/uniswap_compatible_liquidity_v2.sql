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