{{ config(
    schema = 'angstrom_ethereum'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'pair_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    angstrom_decoding_pool_updates(
        angstrom_contract_addr = '0x0000000aa232009084Bd71A5797d089AA4Edfad4'
        , earliest_block = '22971781'
        , blockchain = 'ethereum'
    )
}}
