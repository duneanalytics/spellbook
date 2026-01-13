{{ config(
    schema = 'uniswap'
    , alias = 'uniswapx_trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'[
                                        "ethereum"
                                        , "arbitrum"
                                        , "unichain"
                                        , "base"
                                    ]\',
                                    "project",
                                    "uniswap",
                                    \'["Henrystats", "agaperste"]\') }}')
}}


    {{
        enrich_dex_trades(
            base_trades = ref('uniswap_uniswapx_base_trades')
            , filter = '1 = 1'
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}


-- refresh