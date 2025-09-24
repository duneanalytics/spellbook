{{ config(
    schema = 'angstrom_ethereum'
    , alias = 'trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    angstrom_downstream_trades(
          blockchain = 'ethereum'
        , trades_table = ref('angstrom_ethereum_base_trades')
        , version = '1'
        , project = 'angstrom'
    )
}}