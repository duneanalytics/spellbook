{{ config(
  materialized='incremental',
  unique_key=['user', 'bridge_tx_hash', 'bridge_block_number', 'bridge_block_time'],
  schema='bridge_user_tracking',
  alias='pre_post_bridge_mantle'
) }}

{{ stargate_pre_post_bridge('mantle') }} 