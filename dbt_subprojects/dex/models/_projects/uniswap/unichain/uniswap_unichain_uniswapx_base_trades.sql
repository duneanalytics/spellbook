{{ config(
    schema = 'uniswap_unichain'
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
          blockchain = 'unichain'
          , uniswapx_contracts = ['0x00000006021a6Bce796be7ba509BBBA71e956e37']
          , start_date = '2025-01-01'
          , native_token_address = '0x0000000000000000000000000000000000000000'
    )
}}