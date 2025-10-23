{% macro fluid_liquidity_pools( 
    blockchain = null
    , project = 'fluid'
    , version = null 
    , weth_address = null 
    )
%}

with
decoded_events as (
    select 
        block_time,
        block_number,
        index as evt_index,
        tx_hash,
        contract_address as factory,
        substr(topic1, 13) as dex,
        substr(topic2, 13) as supplyToken,
        substr(topic3, 13) as borrowToken,
        bytearray_to_uint256(data) as dexId
    from 
    {{ source(blockchain, 'logs') }}
    where block_time >= date '2024-09-01' -- dex launch month
    and topic0 = 0x3fecd5f7aca6136a20a999e7d11ff5dcea4bd675cb125f93ccd7d53f98ec57e4 
    -- DexT1Deployed -> sample tx: https://etherscan.io/tx/0xabf5c0e676e69de941c283400d7ac5f47b17a09d870f225b5240522f95da501c#eventlog
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% endif %}
)

select 
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    '{{version}}' as version,
    block_time,
    block_number,
    evt_index,
    tx_hash,
    factory,
    dex,
    if (supplytoken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, {{weth_address}}, supplyToken) as supply_token,
    if (borrowtoken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, {{weth_address}}, borrowToken) as borrow_token,
    dexId as dex_id
from 
decoded_events

{% endmacro %}

{% macro fluid_liquidity_pools_initializations( 
    blockchain = null
    , project = 'fluid'
    , version = null 
    , liquidity_pools = null 
    )
%}
with

decoded_events as (
    select 
        bl.block_time
        , bl.block_number
        , bl.index as evt_index
        , bl.tx_hash
        , fp.dex
        , fp.dex_id
        , bytearray_to_uint256(bytearray_substring(data,129,32))/1e6 as fee
        , bytearray_to_uint256(bytearray_substring(data,1,32)) as smart_col
        , bytearray_to_uint256(bytearray_substring(data,33,32)) as smart_debt
        , bytearray_to_uint256(bytearray_substring(data,161,32))/1e4 as revenue_cut
    from 
    {{ source(blockchain, 'logs') }} bl 
    inner join 
    {{ liquidity_pools }} fp 
        on fp.blockchain = '{{blockchain}}'
        and bl.contract_address = fp.dex 
        and bl.topic0 = 0xa2bd88124c6cef5f905c8de83ae31b7495469be7a44f0a14ec766e4f2926b9e4
    where block_date >= date '2024-09-01' -- dex launch month
    {% if is_incremental() %}
        and {{ incremental_predicate('bl.block_date') }}
    {% endif %}
)

select 
    '{{blockchain}}' as blockchain
    , '{{project}}' as project
    , '{{version}}' as version
    , block_time
    , block_number
    , evt_index
    , tx_hash
    , dex
    , dex_id
    , fee
    , revenue_cut
    , if (smart_col = uint256 '1', true, false) as isSmartCol
    , if (smart_debt = uint256 '1', true, false) as isSmartDebt

from 
decoded_events

{% endmacro %}