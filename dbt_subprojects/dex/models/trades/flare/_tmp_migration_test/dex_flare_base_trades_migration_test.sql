{{ config(
    schema = 'dex_flare_migration_test'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , tags = ['migration_test']
    )
}}

{% set base_models = [
    ref('enosys_v2_flare_base_trades_migration_test')
    , ref('sparkdex_v2_flare_base_trades_migration_test')
    , ref('sparkdex_v3_flare_base_trades_migration_test')
    , ref('blazeswap_flare_base_trades_migration_test')
] %}

{{ dex_base_trades_macro(
    blockchain = 'flare'
    , base_models = base_models
) }}
