{{ config(
  schema='relay',
  alias='relay_token_transfers_zkevm',
  materialized='incremental',
  unique_key=['evt_tx_hash', 'evt_block_time', 'to', 'symbol', 'value']
) }}

{{ relay_token_transfers('zkevm') }} 