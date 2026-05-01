{{ config(
    schema = 'dex_sei'
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
    ref('jelly_swap_sei_base_trades')
    , ref('oku_sei_base_trades')
    , ref('dragon_swap_sei_base_trades')
    , ref('dragon_swap_v3_sei_base_trades')
    , ref('xei_finance_sei_base_trades')
    , ref('carbon_defi_sei_base_trades')
    , ref('yaka_sei_base_trades')
    , ref('sailor_finance_sei_base_trades')
    , ref('yeiswap_sei_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'sei',
    base_models = base_models
) }}
