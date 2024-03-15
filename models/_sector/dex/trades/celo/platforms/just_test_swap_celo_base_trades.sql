{{
  config(
    schema = 'just_test_swap_celo',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
    generic_spot_compatible_trades(
        blockchain = 'celo',
        project = 'just_test_swap',
        version = '1',
        source_evt_swap = source('carbonhood_celo', 'Router_evt_Swap')
    )
}}
