{% set blockchain = 'tron' %}

{{
  config(
    schema = 'bridges_' + blockchain,
    alias = 'allbridge_classic_deposits',
    materialized = 'view',
  )
}}

{% set events %}
  select
    evt_block_date,
    evt_block_time,
    evt_block_number,
    amount,
    sender,
    cast(null as varbinary) as recipient,
    concat(token, 0x000000000000000000000000) as tokenSourceAddress,
    evt_tx_from,
    evt_tx_hash,
    evt_index,
    contract_address,
    evt_index as lockId,
    cast(null as varbinary) as destination
  from {{ source('allbridge_' + blockchain, 'pool_evt_swappedtovusd') }}
{% endset %}

{{
  allbridge_classic_deposits(
    blockchain = blockchain,
    events = events
  )
}}
