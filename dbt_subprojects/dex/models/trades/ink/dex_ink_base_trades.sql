{{ config(
    schema = 'dex_ink'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set base_models = [
    ref('uniswap_v3_ink_base_trades')
    , ref('uniswap_v4_ink_base_trades')
    , ref('inkyswap_ink_base_trades')
    , ref('dyorswap_ink_base_trades')
    , ref('squidswap_ink_base_trades')
    , ref('inkswap_ink_base_trades')
    , ref('reservoir_swap_ink_base_trades')
    , ref('velodrome_v3_ink_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'ink',
    base_models = base_models
) }}
