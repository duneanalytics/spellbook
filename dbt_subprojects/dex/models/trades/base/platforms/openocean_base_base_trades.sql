{{ config(
    schema = 'openocean_base'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    openocean_compatible_v2_trades(
          blockchain = 'base'
        , project = 'openocean'
        , version = '2'
        , evt_swapped = source('openocean_v2_base', 'OpenOceanExchange_evt_Swapped')
        , burn = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        , w_native = '0x4200000000000000000000000000000000000006'
    )
}}