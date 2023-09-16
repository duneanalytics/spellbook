{{ 
    config(
        tags = ['dunesql'],
        schema = 'transfers_celo',
        alias = alias('native'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'transfer_type', 'evt_index', 'wallet_address'],
        post_hook = '{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

with

transfer_raw as (
  select
    'trace received' as transfer_type,
    tx_hash,
    trace_address, 
    block_time,
    to as wallet_address, 
    cast(value as double) as amount_raw
  from {{ source('celo', 'traces') }}
  --where call_type = 'call'
  where (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    and success = true
    and tx_success = true
    and cast(value as double) > 0
    and to is not null
    {% if is_incremental() %}
    and block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  
  union all
  
  select
   'trace sent' as transfer_type,
    tx_hash,
    trace_address, 
    block_time,
    to as wallet_address, 
    -1 * cast(value as double) as amount_raw
  from {{ source('celo', 'traces') }}
  --where call_type = 'call'
  where (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    and success = true
    and tx_success = true
    and cast(value as double) > 0
    and "from" is not null
    {% if is_incremental() %}
    and block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  
  union all
  
  select
    'gas fee' as transfer_type, 
    tx_hash,
    array[index] as trace_address,
    block_time,
    "from" as wallet_address,
    -1 * cast(gas_price as double) * cast(gas_used as double) as amount_raw
  from {{ source('celo', 'transactions') }}
  where success = true
    {% if is_incremental() %}
    and block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  
  union all
  
  select
   'transfer received' as transfer_type,
    evt_tx_hash as tx_hash,
    array[evt_index] as trace_address,
    block_time,
    to as wallet_address,
    cast(value as double) as amount_raw
  from {{ source('erc20_celo', 'evt_transfer') }}
  where contract_address = 0x471ece3750da237f93b8e339c536989b8978a438 -- CELO native asset
    {% if is_incremental() %}
    and evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  
  union all
  
  select
   'transfer sent' as transfer_type,
    evt_tx_hash as tx_hash,
    array[evt_index] as trace_address,
    block_time,
    "from" as wallet_address,
    -1 * cast(value as double) as amount_raw
  from {{ source('erc20_celo', 'evt_transfer') }}
  where contract_address = 0x471ece3750da237f93b8e339c536989b8978a438 -- CELO native asset
    {% if is_incremental() %}
    and evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

select
  'celo' as blockchain, 
  transfer_type,
  wallet_address,
  0x471ece3750da237f93b8e339c536989b8978a438 as token_address,
  block_time,
  cast(date_trunc('month', block_time) as date) as block_month,
  tx_hash,
  trace_address,
  amount_raw
from transfer_raw
