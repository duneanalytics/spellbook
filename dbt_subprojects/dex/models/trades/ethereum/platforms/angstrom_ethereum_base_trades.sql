{{ config(
    schema = 'angstrom_ethereum'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , pre_hook = [
        "SET SESSION query_max_stage_count = 1000",
        "SET SESSION max_recursion_depth = 35"
      ]
    )
}}

{{
    angstrom_all_trades(
        angstrom_contract_addr = '0x0000000aa232009084Bd71A5797d089AA4Edfad4',
        controller_v1_contract_addr = '0x1746484EA5e11C75e009252c102C8C33e0315fD4',
        earliest_block = '22971781',
        blockchain = 'ethereum',
        project = 'uniswap',
        version = '4',
        controller_pool_configured_log_topic0 = '0xf325a037d71efc98bc41dc5257edefd43a1d1162e206373e53af271a7a3224e9',
        bundle_tob_orders_table = ref('angstrom_ethereum_bundle_tob_orders'),
        bundle_user_orders_table = ref('angstrom_ethereum_bundle_user_orders'),
        composable_orders_table = ref('angstrom_ethereum_composable_orders')
    )
}}
