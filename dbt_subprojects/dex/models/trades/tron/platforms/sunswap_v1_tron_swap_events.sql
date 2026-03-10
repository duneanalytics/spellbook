{{
  config(
    schema = 'sunswap_v1_tron',
    alias = 'swap_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_tx_hash', 'evt_index', 'contract_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
    partition_by = ['evt_block_date']
  )
}}

{% set token_purchase_abi %}
{
  "anonymous": false,
  "inputs": [
    { "indexed": true, "internalType": "address", "name": "buyer", "type": "address" },
    { "indexed": true, "internalType": "uint256", "name": "trx_sold", "type": "uint256" },
    { "indexed": true, "internalType": "uint256", "name": "tokens_bought", "type": "uint256" }
  ],
  "name": "TokenPurchase",
  "type": "event"
}
{% endset %}

{% set trx_purchase_abi %}
{
  "anonymous": false,
  "inputs": [
    { "indexed": true, "internalType": "address", "name": "buyer", "type": "address" },
    { "indexed": true, "internalType": "uint256", "name": "tokens_sold", "type": "uint256" },
    { "indexed": true, "internalType": "uint256", "name": "trx_bought", "type": "uint256" }
  ],
  "name": "TrxPurchase",
  "type": "event"
}
{% endset %}

with exchanges as (
  select
    f.exchange,
    f.token
  from {{ source('sunswap_v1_tron', 'justswapfactory_evt_newexchange') }} as f
),

token_purchase as (
  select
    'token_purchase' as swap_type,
    contract_address,
    tx_hash as evt_tx_hash,
    cast(index as bigint) as evt_index,
    block_time as evt_block_time,
    cast(block_number as bigint) as evt_block_number,
    cast(block_date as date) as evt_block_date,
    cast(buyer as varbinary) as buyer,
    cast(tokens_bought as uint256) as token_amount,
    cast(trx_sold as uint256) as trx_amount
  from table(
    decode_evm_event(
      abi => '{{ token_purchase_abi | trim }}',
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
        inner join exchanges as e
          on e.exchange = l.contract_address
        where l.topic0 = keccak(to_utf8('TokenPurchase(address,uint256,uint256)'))
        {% if is_incremental() %}
          and {{ incremental_predicate('l.block_time') }}
        {% endif %}
      )
    )
  ) as r
),

trx_purchase as (
  select
    'trx_purchase' as swap_type,
    contract_address,
    tx_hash as evt_tx_hash,
    cast(index as bigint) as evt_index,
    block_time as evt_block_time,
    cast(block_number as bigint) as evt_block_number,
    cast(block_date as date) as evt_block_date,
    cast(buyer as varbinary) as buyer,
    cast(tokens_sold as uint256) as token_amount,
    cast(trx_bought as uint256) as trx_amount
  from table(
    decode_evm_event(
      abi => '{{ trx_purchase_abi | trim }}',
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
        inner join exchanges as e
          on e.exchange = l.contract_address
        where l.topic0 = keccak(to_utf8('TrxPurchase(address,uint256,uint256)'))
        {% if is_incremental() %}
          and {{ incremental_predicate('l.block_time') }}
        {% endif %}
      )
    )
  ) as r
)

select
  s.swap_type,
  s.contract_address,
  s.evt_tx_hash,
  s.evt_index,
  s.evt_block_time,
  s.evt_block_number,
  s.evt_block_date,
  s.buyer,
  s.token_amount,
  s.trx_amount
from token_purchase as s
union all
select
  s.swap_type,
  s.contract_address,
  s.evt_tx_hash,
  s.evt_index,
  s.evt_block_time,
  s.evt_block_number,
  s.evt_block_date,
  s.buyer,
  s.token_amount,
  s.trx_amount
from trx_purchase as s
