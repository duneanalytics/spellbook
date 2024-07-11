{{
  config(
    schema = 'morpho_blue_ethereum',
    alias = 'base_supply',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{{
  lending_aave_v3_compatible_borrow(
    blockchain = 'ethereum',
    project = 'morpho',
    version = 'blue',
    project_decoded_as = 'morpho_blue',
    decoded_contract_name = 'MorphoBlue',
    liquidate_event_name = 'evt_Liquidate'
  )
}}
