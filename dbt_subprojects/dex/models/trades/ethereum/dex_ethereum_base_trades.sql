{{ config(
    schema = 'dex_ethereum'
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
    ref('defiswap_ethereum_base_trades')
    , ref('uniswap_v4_ethereum_base_trades')
    , ref('uniswap_v2_ethereum_base_trades')
    , ref('uniswap_v3_ethereum_base_trades')
    , ref('uniswap_v1_ethereum_base_trades')
    , ref('apeswap_ethereum_base_trades')
    , ref('carbon_defi_ethereum_base_trades')
    , ref('airswap_ethereum_base_trades')
    , ref('sushiswap_v1_ethereum_base_trades')
    , ref('sushiswap_v2_ethereum_base_trades')
    , ref('pancakeswap_v2_ethereum_base_trades')
    , ref('pancakeswap_v3_ethereum_base_trades')
    , ref('shibaswap_v1_ethereum_base_trades')
    , ref('balancer_v1_ethereum_base_trades')
    , ref('balancer_v2_ethereum_base_trades')
    , ref('balancer_v3_ethereum_base_trades')    
    , ref('fraxswap_ethereum_base_trades')
    , ref('bancor_ethereum_base_trades')
    , ref('verse_dex_ethereum_base_trades')
    , ref('swapr_ethereum_base_trades')
    , ref('mauve_ethereum_base_trades')
    , ref('dfx_ethereum_base_trades')
    , ref('dodo_ethereum_base_trades')
    , ref('integral_ethereum_base_trades')
    , ref('maverick_ethereum_base_trades')
    , ref('maverick_v2_ethereum_base_trades')
    , ref('kyberswap_ethereum_base_trades')
    , ref('clipper_ethereum_base_trades')
    , ref('mstable_ethereum_base_trades')
    , ref('xchange_ethereum_base_trades')
    , ref('curve_ethereum_base_trades')
    , ref('solidly_v3_ethereum_base_trades')
    , ref('swaap_v2_ethereum_base_trades')
    , ref('valantis_hot_ethereum_base_trades')
    , ref('xchange_v2_ethereum_base_trades')
    , ref('fluid_v1_ethereum_base_trades')
    , ref('native_ethereum_base_trades')
    , ref('eulerswap_ethereum_base_trades')
    , ref('ekubo_v1_ethereum_base_trades')
    , ref('ekubo_v3_ethereum_base_trades')
    , ref('angstrom_ethereum_base_trades')
    , ref('supernova_v2_ethereum_base_trades')
    , ref('supernova_v3_ethereum_base_trades')
] %}
WITH base AS (
    {{ dex_base_trades_macro(
        blockchain = 'ethereum',
        base_models = base_models
    ) }}
)
SELECT * FROM base
WHERE tx_hash != 0x1c27c4d625429acfc0f97e466eda725fd09ebdc77550e529ba4cbdbc33beb97b
