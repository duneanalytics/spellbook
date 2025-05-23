{{ config(
  materialized='incremental',
  unique_key=['block_time', 'block_number', 'user', 'dest', 'amount_raw'],
  schema='stargate',
  alias='stargate_bridge_transfers_base'
) }}

{{ stargate_bridge_transfers('base') }} 