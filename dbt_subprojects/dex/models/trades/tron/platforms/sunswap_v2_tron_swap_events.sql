{{
  config(
    schema = 'sunswap_v2_tron',
    alias = 'swap_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_tx_hash', 'evt_index', 'contract_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
    partition_by = ['evt_block_date']
  )
}}

{% set swap_event_abi %}
{
  "anonymous": false,
  "inputs": [
    { "indexed": true, "internalType": "address", "name": "sender", "type": "address" },
    { "indexed": false, "internalType": "uint256", "name": "amount0In", "type": "uint256" },
    { "indexed": false, "internalType": "uint256", "name": "amount1In", "type": "uint256" },
    { "indexed": false, "internalType": "uint256", "name": "amount0Out", "type": "uint256" },
    { "indexed": false, "internalType": "uint256", "name": "amount1Out", "type": "uint256" },
    { "indexed": true, "internalType": "address", "name": "to", "type": "address" }
  ],
  "name": "Swap",
  "type": "event"
}
{% endset %}

{% set swap_topic0 = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' %}

with pairs as (
  select
    f.pair
  from {{ source('sunswap_v2_tron', 'sunswapv2factory_evt_paircreated') }} as f
),

raw_decoded as (
  select
    *
  from table(
    decode_evm_event(
      abi => '{{ swap_event_abi | trim }}',
      input => table(
        select
          l.topic0,
          l.topic1,
          l.topic2,
          l.topic3,
          l.data,
          l.contract_address,
          l.tx_hash,
          l.tx_from,
          l.tx_to,
          l.index,
          l.block_time,
          l.block_number,
          l.block_date
        from {{ source('tron', 'logs') }} as l
        inner join pairs as p
          on p.pair = l.contract_address
        where l.topic0 = {{ swap_topic0 }}
        {% if is_incremental() %}
          and {{ incremental_predicate('l.block_time') }}
        {% endif %}
      )
    )
  ) as r
),

decoded as (
  select
    r.contract_address,
    r.tx_hash as evt_tx_hash,
    r.tx_from as evt_tx_from,
    r.tx_to as evt_tx_to,
    cast(r.index as integer) as evt_tx_index,
    cast(r.index as bigint) as evt_index,
    r.block_time as evt_block_time,
    cast(r.block_number as bigint) as evt_block_number,
    cast(r.block_date as date) as evt_block_date,
    cast(r.amount0In as uint256) as amount0In,
    cast(r.amount1In as uint256) as amount1In,
    cast(r.amount0Out as uint256) as amount0Out,
    cast(r.amount1Out as uint256) as amount1Out,
    r.sender,
    r.to
  from raw_decoded as r
)

select
  d.contract_address,
  d.evt_tx_hash,
  d.evt_tx_from,
  d.evt_tx_to,
  d.evt_tx_index,
  d.evt_index,
  d.evt_block_time,
  d.evt_block_number,
  d.evt_block_date,
  d.amount0In,
  d.amount1In,
  d.amount0Out,
  d.amount1Out,
  d.sender,
  d.to
from decoded as d
