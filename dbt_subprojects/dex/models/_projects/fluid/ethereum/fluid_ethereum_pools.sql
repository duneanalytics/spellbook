{{
    config(
        schema = 'fluid_ethereum',
        alias = 'pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['dex'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

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
    from {{ source('ethereum', 'logs')}}
    where topic0 = 0x3fecd5f7aca6136a20a999e7d11ff5dcea4bd675cb125f93ccd7d53f98ec57e4 
    -- DexT1Deployed -> sample tx: https://etherscan.io/tx/0xabf5c0e676e69de941c283400d7ac5f47b17a09d870f225b5240522f95da501c#eventlog
    and block_number > 20776998
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

select 
    'ethereum' as blockchain,
    'fluid' as project,
    block_time,
    block_number,
    evt_index,
    tx_hash,
    factory,
    dex,
    case supplyToken when 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then {{weth_address}} else supplyToken end as supply_token,
    case borrowToken when 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then {{weth_address}} else borrowToken end as borrow_token,
    dexId as dex_id
from decoded_events