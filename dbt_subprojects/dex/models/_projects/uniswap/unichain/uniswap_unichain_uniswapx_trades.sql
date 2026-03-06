{{ config(
    schema = 'uniswap_unichain'
    , alias = 'uniswapx_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_month', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

    {{
        enrich_dex_trades(
            base_trades = ref('uniswap_unichain_uniswapx_base_trades')
            , filter = '1 = 1'
            , tokens_erc20_model = source('tokens', 'erc20')
            , blockchain = 'unichain'
        )
    }}