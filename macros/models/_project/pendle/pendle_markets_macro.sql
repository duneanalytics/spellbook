{%
  macro pendle_markets(
    blockchain = '',
    project = '',
    version = '',
    project_decoded_as = '',
    create_market_table = '',
    create_yield_table = '',
    start_date = '2022-11-23'
  )
%}

with 
markets as (
    select 
        m.evt_block_time, m.evt_tx_hash, m.evt_index,
        '{{blockchain}}' as chain,
        '{{version}}' as version,
        m.market,
        m.PT,
        y.expiry,
        y.SY,
        y.YT 
    from {{ source(project_decoded_as ~ '_' ~ blockchain, create_market_table) }} m
        join {{ source(project_decoded_as ~ '_' ~ blockchain, create_yield_table) }} y 
        on m.PT = y.PT
    {% if is_incremental() %}
    where {{ incremental_predicate('m.evt_block_time') }}
    {% endif %} 
),
calls as (
    select *  
    from {{ source(blockchain, 'traces') }}
    where 1=1
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %} 
    and block_time > date'{{ start_date }}'
    and to in (
        select SY from markets
    )
    and input in (
        0xa40bee50, -- assetInfo()
        0x95d89b41  -- symbol()
    )
),
asset_info_calls as (
    select 
        c1.block_time,c1.block_number,c1.trace_address,c1.tx_hash,c1.to as contract_address,
        
        bytearray_to_int256 (bytearray_substring (c1.output, 1+(0*32), 32)) as asset_type,
        substr (bytearray_substring (c1.output, 1+(1*32), 32), 13, 20) as asset,
        bytearray_to_int256 (bytearray_substring (c1.output, 1+(2*32), 32)) as decimals,
        from_utf8(varbinary_rtrim(bytearray_substring (c2.output, 1+(2*32), 32))) as symbol
    from calls c1
        join calls c2
            on c1.input = 0xa40bee50 
            and c2.input = 0x95d89b41 
            and c1.tx_hash = c2.tx_hash
),
joined_data as (

    {% if is_incremental() %}
    select 
        market,
        expiry,
        pt,
        sy,
        yt,
        asset,
        decimals,
        sy_symbol
    from {{this}}
    union all
    {% endif %} 

    select
        m.market,
        m.expiry,
        m.pt,
        m.sy,
        m.yt,
        u.asset,
        u.decimals,
        u.symbol as sy_symbol
    from markets m 
        join asset_info_calls u 
            on m.sy = u.contract_address
),
market_list as (
    {#
        all markets are manually deployed, so there are duplicating issues
    #}
    select distinct * from joined_data
    where asset is not null
    {% if is_incremental() %}
    and m.market not in (select m.market from {{ this }})
    {% endif %} 
)

select 
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    '{{version}}' as version,
    market,
    expiry,
    sy,
    pt,
    yt,
    asset,
    decimals,
    sy_symbol
from market_list

{% endmacro %}