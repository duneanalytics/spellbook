{{ config(
    schema = 'angstrom_ethereum'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'pair_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , pre_hook = [
        "SET SESSION query_max_stage_count = 1000",
        "SET SESSION max_recursion_depth = 35",
        "SET SESSION distinct_aggregations_strategy = 'single_step'"
      ]
    )
}}

{{
    angstrom_decoding_pool_updates(
        angstrom_contract_addr = '0x0000000aa232009084Bd71A5797d089AA4Edfad4'
        , earliest_block = '22971781'
        , blockchain = 'ethereum'
    )
}}
