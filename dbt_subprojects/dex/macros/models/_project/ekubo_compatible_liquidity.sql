{% macro ekubo_compatible_liquidity_events( 
    blockchain = null
    , project = null 
    , version = null 
    , liquidity_pools = null
    , start_block_number = null 
    , ekubo_core_contract = null 
    , position_updated = null 
    , position_fees_collected = null 
    )
%}


with 

swap_events as (
    select 
        el.block_time,
        el.block_number,
        el.tx_from,
        substr(el."data", 21, 32) AS id,
        cast(varbinary_to_decimal(substr(el."data", 53, 16)) as decimal(38,0)) as amount0,
        cast(varbinary_to_decimal(substr(el."data", 69, 16)) as decimal(38,0)) as amount1,
        el.tx_hash,
        el.index as evt_index,
        'swap' as event_type
    from 
    {{ source (blockchain, 'logs') }} el 
    where block_number >= {{ start_block_number }}
    and contract_address = {{ ekubo_core_contract }}
    and topic0 is null 
    {%- if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {%- endif %} 
),

liquidity_events as (
    select 
        evt_block_time as block_time,
        evt_block_number as block_number,
        evt_tx_from as tx_from,
        poolId as id,
        delta0 as amount0,
        delta1 as amount1,
        evt_tx_hash as tx_hash,
        evt_index,
        'modify_liquidity' as event_type 
    from 
    {{ position_updated }}
    {%- if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {%- endif %} 

    union all 

    select 
        evt_block_time as block_time,
        evt_block_number as block_number,
        evt_tx_from as tx_from,
        poolId as id,
        -amount0 as amount0,
        -amount1 as amount1,
        evt_tx_hash as tx_hash,
        evt_index,
        'fees_collected' as event_type 
    from 
    {{ position_fees_collected }}
    {%- if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
),

all_events as (
    select * from swap_events 

    union all 

    select * from liquidity_events
),

get_pools as (
    select 
        blockchain
        , id
        , token0
        , token1
    from 
    {{ liquidity_pools }}
    where blockchain = '{{blockchain}}'
    and version = '{{version}}'
)

    select 
        '{{blockchain}}' as blockchain,
        '{{project}}'  as project,
        '{{version}}' as version,
        cast(date_trunc('month', ae.block_time) as date) as block_month,
        cast(date_trunc('day', ae.block_time) as date) as block_date,
        date_trunc('minute', ae.block_time) as block_time, -- for prices
        ae.block_number,
        ae.id,
        ae.tx_hash,
        ae.tx_from,
        ae.evt_index,
        ae.event_type,
        ep.token0,
        ep.token1,
        CAST(ae.amount0 AS double) as amount0_raw,
        CAST(ae.amount1 AS double) as amount1_raw
    from 
    all_events ae 
    inner join 
    get_pools ep 
        on ae.id = ep.id 

{% endmacro %}