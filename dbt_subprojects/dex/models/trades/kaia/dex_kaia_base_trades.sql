{{ config(
    schema = 'dex_kaia'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

--exclude kaiaswap until enabled ref('kaia_swap_v3_kaia_base_trades')
{% set base_models = [
    ref('dragon_swap_v2_kaia_base_trades')
    , ref('dragon_swap_v3_kaia_base_trades')
    , ref('klay_swap_v3_kaia_base_trades')
    , ref('neopin_kaia_base_trades')
    , ref('defi_kingdoms_kaia_base_trades')
    , ref('cldex_kaia_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'kaia',
    base_models = base_models
) }}
