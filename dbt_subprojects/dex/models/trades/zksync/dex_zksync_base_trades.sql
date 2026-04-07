{{ config(
    schema = 'dex_zksync'
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
    ref('maverick_zksync_base_trades')
    , ref('maverick_v2_zksync_base_trades')
    , ref('pancakeswap_v2_zksync_base_trades')
    , ref('pancakeswap_v3_zksync_base_trades')
    , ref('syncswap_v1_zksync_base_trades')
    , ref('syncswap_v2_zksync_base_trades')
    , ref('uniswap_v3_zksync_base_trades')
    , ref('mute_zksync_base_trades')
    , ref('spacefi_v1_zksync_base_trades')
    , ref('derpdex_v1_zksync_base_trades')
    , ref('ezkalibur_v2_zksync_base_trades')
    , ref('wagmi_v1_zksync_base_trades')
    , ref('zkswap_finance_zksync_base_trades')
    , ref('zkswap_finance_v3_zksync_base_trades')
    , ref('gemswap_zksync_base_trades')
    , ref('vesync_v1_zksync_base_trades')
    , ref('dracula_finance_zksync_base_trades')
    , ref('izumi_finance_v1_zksync_base_trades')
    , ref('izumi_finance_v2_zksync_base_trades')
    , ref('velocore_v0_zksync_base_trades')
    , ref('velocore_v1_zksync_base_trades')
    , ref('velocore_v2_zksync_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'zksync',
    base_models = base_models
) }}
