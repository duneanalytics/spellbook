{% macro ekubo_compatible_pools(
    blockchain = null
    , project = null
    , version = null
    , pool_init = null
    , weth_address = null 
    )
%}

with 

pool_created as (
    select 
        contract_address,
        evt_index, 
        evt_tx_hash as tx_hash,
        evt_block_time as creation_block_time,
        evt_block_number as creation_block_number, 
        poolId as id,
        poolKey as pool_key,
        from_hex(json_extract_scalar(poolkey, '$.token0')) as token0,
        from_hex(json_extract_scalar(poolkey, '$.token1')) as token1,
        from_hex(json_extract_scalar(poolkey, '$.config')) as config
    from 
    {{ pool_init }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

select 
    '{{ blockchain }}' as blockchain,
    '{{ project }}' as project,
    '{{ version }}' as version,
    contract_address,
    creation_block_time,
    creation_block_number,
    id,
    ((varbinary_to_int256(substr(config, 21, 8)))/pow(2, 64)) * 100 as fee_decimal,
    (varbinary_to_int256(substr(config, 29, 4)))/pow(10, 4) as tick_spacing_decimal,
    tx_hash,
    evt_index,
    if(token0 = 0x0000000000000000000000000000000000000000, {{ weth_address }}, token0) as token0,
    if(token1 = 0x0000000000000000000000000000000000000000, {{ weth_address }}, token1) as token1
from 
pool_created

{% endmacro %}