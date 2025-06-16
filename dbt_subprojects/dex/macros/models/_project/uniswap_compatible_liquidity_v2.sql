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