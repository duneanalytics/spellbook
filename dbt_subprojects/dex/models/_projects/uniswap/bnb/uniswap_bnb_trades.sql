{{ config(
    schema = 'uniswap_bnb'
    , alias = 'trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_month', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_downstream_trades(
          blockchain = 'bnb'
          , has_univ4 = true
          , has_bunni = true
    )
}}