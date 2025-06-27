{{ config(
  schema='relay',
  alias='relay_token_transfers_sonic',
  materialized='incremental',
  unique_key=['evt_tx_hash', 'evt_block_time', 'to', 'symbol', 'value']
) }}

{{ relay_token_transfers('sonic') }} 