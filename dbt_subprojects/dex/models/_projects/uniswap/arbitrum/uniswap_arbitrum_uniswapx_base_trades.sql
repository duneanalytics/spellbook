{{ config(
    schema = 'uniswap_arbitrum'
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
          blockchain = 'arbitrum'
          , uniswapx_contracts = ['0xB274d5F4b833b61B340b654d600A864fB604a87c', '0x1bd1aAdc9E230626C44a139d7E70d842749351eb']
          , start_date = '2024-05-01'
          , native_token_address = '0x0000000000000000000000000000000000000000'
    )
}}