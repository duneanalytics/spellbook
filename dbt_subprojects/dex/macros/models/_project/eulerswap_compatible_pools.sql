{% macro eulerswap_compatible_univ4_pools(
    blockchain = null
    , project = 'eulerswap'
    , version = '1'
    , uniswap_pools = null 
    , factory_univ4_pooldeployed = null 
    , factory_univ4_poolconfig = null 
    )
%}

select 
    '{{blockchain}}' as blockchain
    , '{{project}}' as project 
    , '{{version}}' as version 
    , d.contract_address as factory_address 
    , d.evt_block_time as creation_block_time
    , d.evt_block_number as creation_block_number 
    , p.id as pool 
    , d.pool as hook -- EulerSwap instance's pool is the hook in Uniswap v4 universe 
    , d.eulerAccount
    , d.asset0
    , d.asset1
    , from_hex(substr(json_extract_scalar(c.params, '$.vault0'), 3)) as vault0
    , from_hex(substr(json_extract_scalar(c.params, '$.vault1'), 3)) as vault1
    , cast(json_extract_scalar (c.params, '$.fee') as double) / 1e18 as fee
    , cast(json_extract_scalar (c.params, '$.fee') as double) / 1e18 as protocolFee
    , from_hex(substr(json_extract_scalar(c.params, '$.protocolFeeRecipient'), 3)) as protocolFeeRecipient
from 
{{ factory_univ4_pooldeployed }} d
left join 
{{ factory_univ4_poolconfig }} c 
    on d.evt_block_number = c.evt_block_number 
    and d.evt_tx_hash = c.evt_tx_hash 
    and d.pool = c.pool 
    {%- if is_incremental() %}
    and {{ incremental_predicate('c.evt_block_time') }}
    {%- endif %}
left join 
{{ uniswap_pools }} p 
    on p.blockchain = '{{blockchain}}'
    and d.evt_block_number = p.creation_block_number
    and d.pool = p.hooks 
{%- if is_incremental() %}
where {{ incremental_predicate('d.evt_block_time') }}
{%- endif %}

{% endmacro %}

{% macro eulerswap_compatible_pools_prep(
    blockchain = null
    , project = 'eulerswap'
    , version = '1'
    , PoolCreations = null
    )
%}

{% if is_incremental() %}

with

pool_creations as (
    select 
        'include' as check_filter
        , factory_address 
        , creation_block_time
        , creation_block_number 
        , pool 
        , hook 
        , eulerAccount
        , asset0
        , asset1
        , vault0
        , vault1
        , fee
        , protocolFee
        , protocolFeeRecipient
    from 
    {{ PoolCreations }}
    where {{ incremental_predicate('creation_block_time') }}
),

get_active_pools as ( -- get only the pools that were active on incremental run
    select 
        distinct pool 
        , eulerAccount
    from 
    pool_creations
),

get_latest_active_pools as (
    select 
         'exclude' as check_filter
        , max_by(factory_address, creation_block_number) as factory_address
        , max_by(creation_block_time, creation_block_number) as creation_block_time
        , max(creation_block_number) as creation_block_number
        , pool 
        , max_by(hook  , creation_block_number) as hook 
        , eulerAccount
        , max_by(asset0 , creation_block_number) as asset0
        , max_by(asset1 , creation_block_number) as asset1
        , max_by(vault0 , creation_block_number) as vault0
        , max_by(vault1 , creation_block_number) as vault1
        , max_by(fee , creation_block_number) as fee
        , max_by(protocolFee , creation_block_number) as protocolFee
        , max_by(protocolFeeRecipient , creation_block_number) as protocolFeeRecipient
    from (
        select 
            th.*,
            pc.creation_block_number as exclude_block_number
        from 
        {{this}} th 
        inner join 
        get_active_pools ga 
            on th.pool = ga.pool 
            and th.eulerAccount = ga.eulerAccount
        left join 
        pool_creations pc 
            on th.pool = pc.pool 
            and th.eulerAccount = pc.eulerAccount
    ) th 
    where exclude_block_number is null 
    group by 1, 5, 7 

    union all 

    select 
         check_filter
        , factory_address 
        , creation_block_time
        , creation_block_number 
        , pool 
        , hook 
        , eulerAccount
        , asset0
        , asset1
        , vault0
        , vault1
        , fee
        , protocolFee
        , protocolFeeRecipient
    from 
    pool_creations
), 

sort_table as (
    select 
        *
        , lag(block_number, 1, 0) over (partition by eulerAccount, pool order by block_number) as previous_block_number 
    from 
    get_latest_active_pools
)

    select 
        '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , factory_address 
        , creation_block_time
        , creation_block_number 
        , previous_block_number
        , pool 
        , hook 
        , eulerAccount
        , asset0
        , asset1
        , vault0
        , vault1
        , fee
        , protocolFee
        , protocolFeeRecipient
    from 
    sort_table 
    where check_filter = 'include'


{% else %}

with

pool_creations as (
    select 
        , factory_address 
        , creation_block_time
        , creation_block_number 
        , pool 
        , hook 
        , eulerAccount
        , asset0
        , asset1
        , vault0
        , vault1
        , fee
        , protocolFee
        , protocolFeeRecipient
    from 
    {{ PoolCreations }}
),

sort_table as (
    select 
        *
        , lag(block_number, 1, 0) over (partition by eulerAccount, pool order by block_number) as previous_block_number 
    from 
    pool_creations
)

    select 
        '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , factory_address 
        , creation_block_time
        , creation_block_number 
        , previous_block_number
        , pool 
        , hook 
        , eulerAccount
        , asset0
        , asset1
        , vault0
        , vault1
        , fee
        , protocolFee
        , protocolFeeRecipient
    from 
    sort_table 

{% endif %}

{% endmacro %}