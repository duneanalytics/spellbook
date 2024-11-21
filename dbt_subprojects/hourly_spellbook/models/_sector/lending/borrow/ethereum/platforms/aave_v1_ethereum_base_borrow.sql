{{
  config(
    schema = 'aave_v1_ethereum',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v1_compatible_borrow(
    blockchain = 'ethereum',
    project = 'aave',
    version = '1',
    aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    native_token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  )
}}
