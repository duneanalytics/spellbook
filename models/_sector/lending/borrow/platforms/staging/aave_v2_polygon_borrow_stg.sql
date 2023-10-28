{{
  config(
    schema = 'aave_v2_polygon',
    alias = 'borrow_stg',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'evt_tx_hash', 'evt_index']
  )
}}

{{
  lending_aave_v2_fork_borrow(
    blockchain = 'polygon',
    project = 'aave',
    version = '2'
  )
}}
