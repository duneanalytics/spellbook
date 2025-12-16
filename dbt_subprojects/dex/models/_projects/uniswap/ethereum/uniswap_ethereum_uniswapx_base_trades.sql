{{ config(
    schema = 'uniswap_ethereum'
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
          blockchain = 'ethereum'
          , uniswapx_contract = '0x00000011F84B9aa48e5f8aA8B9897600006289Be'
          , start_date = '2024-05-01'
          , native_token_address = '0x0000000000000000000000000000000000000000'
    )
}}