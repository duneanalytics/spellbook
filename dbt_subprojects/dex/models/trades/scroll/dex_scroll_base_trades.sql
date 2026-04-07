{{ config(
    schema = 'dex_scroll'
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
    ref('uniswap_v3_scroll_base_trades')
    , ref('sushiswap_v2_scroll_base_trades')
    , ref('zebra_scroll_base_trades')
    , ref('scrollswap_scroll_base_trades')
    , ref('syncswap_v1_scroll_base_trades')
    , ref('nuri_scroll_base_trades')
    , ref('izumi_finance_scroll_base_trades')
    , ref('icecreamswap_v2_scroll_base_trades')
    , ref('maverick_v2_scroll_base_trades')
    , ref('swaap_v2_scroll_base_trades')
    , ref('leetswap_scroll_base_trades')
    , ref('spacefi_scroll_base_trades')
    , ref('punkswap_scroll_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'scroll',
    base_models = base_models
) }}
