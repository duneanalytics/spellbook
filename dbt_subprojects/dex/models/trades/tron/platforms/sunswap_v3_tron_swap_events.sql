{{
  config(
    schema = 'sunswap_tron',
    alias = 'v3pool_evt_swap_spells',
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
    { "indexed": true,  "internalType": "address", "name": "sender",       "type": "address" },
    { "indexed": true,  "internalType": "address", "name": "recipient",    "type": "address" },
    { "indexed": false, "internalType": "int256",  "name": "amount0",      "type": "int256" },
    { "indexed": false, "internalType": "int256",  "name": "amount1",      "type": "int256" },
    { "indexed": false, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160" },
    { "indexed": false, "internalType": "uint128", "name": "liquidity",    "type": "uint128" },
    { "indexed": false, "internalType": "int24",   "name": "tick",         "type": "int24" }
  ],
  "name": "Swap",
  "type": "event"
}
{% endset %}

{% set swap_topic0 = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67' %}

with pools as (
  select
    f.pool
  from {{ source('sunswap_tron', 'v3factory_evt_poolcreated') }} f
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
        from {{ source('tron', 'logs') }} l
        inner join pools p on p.pool = l.contract_address
        where l.topic0 = {{ swap_topic0 }}
        {% if is_incremental() %}
          and {{ incremental_predicate('l.block_time') }}
        {% endif %}
      )
    )
  ) r
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
    r.amount0,
    r.amount1,
    cast(r.liquidity as uint256) as liquidity,
    r.recipient,
    r.sender,
    cast(r.sqrtPriceX96 as uint256) as sqrtPriceX96,
    cast(r.tick as integer) as tick
  from raw_decoded r
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
  d.amount0,
  d.amount1,
  d.liquidity,
  d.recipient,
  d.sender,
  d.sqrtPriceX96,
  d.tick
from decoded d
