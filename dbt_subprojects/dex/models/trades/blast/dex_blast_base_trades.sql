{{ config(
    schema = 'dex_blast'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}

{% set base_models = [
    ref('uniswap_v4_blast_base_trades')
    , ref('uniswap_v3_blast_base_trades')
    , ref('uniswap_v2_blast_base_trades')
    , ref('blasterswap_blast_base_trades')
    , ref('thruster_blast_base_trades')
    , ref('fenix_blast_base_trades')
    , ref('dackieswap_v2_blast_base_trades')
    , ref('sushiswap_v2_blast_base_trades')
    , ref('dackieswap_v3_blast_base_trades')
    , ref('swapblast_blast_base_trades')
    , ref('dyorswap_blast_base_trades')
    , ref('icecreamswap_v2_blast_base_trades')
] %}

{{ dex_base_trades_macro(
    blockchain = 'blast',
    base_models = base_models
) }}
