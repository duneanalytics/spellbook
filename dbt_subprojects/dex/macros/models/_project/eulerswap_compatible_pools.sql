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

{% macro eulerswap_compatible_pools(
    blockchain = null
    , project = 'eulerswap'
    , version = '1'
    , PoolCreations = null
    )
%}


with

pool_creations as (
    select 
        factory_address 
        , creation_block_time
        , creation_block_number 
        , lead(creation_block_number, 1, 1e18) over (partition by eulerAccount order by creation_block_number) as next_block_number
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
    where version = '{{version}}'
)

    select 
        '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , factory_address 
        , creation_block_time
        , creation_block_number 
        , next_block_number
        , pool 
        , case when next_block_number = 1e18 then true else false end as isActive
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

{% endmacro %}