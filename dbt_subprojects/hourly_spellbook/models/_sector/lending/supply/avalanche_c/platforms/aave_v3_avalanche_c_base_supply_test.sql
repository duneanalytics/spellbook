{{
  config(
    schema = 'aave_v3_avalanche_c',
    alias = 'base_supply_test',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

-- just a test to trigger re-run
select 1 where 1=0
