{{ config(
    schema = 'uniswap_base'
    , alias = 'uniswapx_base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_month', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_uniswapx_trades(
          blockchain = 'base'
          , uniswapx_contracts = ['0x000000001Ec5656dcdB24D90DFa42742738De729']
          , start_date = '2024-08-01'
          , native_token_address = '0x0000000000000000000000000000000000000000'
    )
}}