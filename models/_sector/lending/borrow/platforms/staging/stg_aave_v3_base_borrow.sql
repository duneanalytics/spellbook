{{
  config(
    schema = 'aave_v3_base',
    alias = 'stg_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['token_address', 'evt_tx_hash', 'evt_block_number', 'evt_index']
  )
}}

{{
  lending_aave_v3_fork_borrow(
    blockchain = 'base',
    project = 'aave',
    version = '3'
  )
}}
