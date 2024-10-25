{{ config(
    schema = 'dex'
    , alias = 'automated_trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'[
                                        "arbitrum"
                                        , "avalanche_c"
                                        , "base"
                                        , "blast"
                                        , "bnb"
                                        , "celo"
                                        , "ethereum"
                                        , "fantom"
                                        , "gnosis"
                                        , "linea"
                                        , "mantle"
                                        , "nova"
                                        , "optimism"
                                        , "polygon"
                                        , "scroll"
                                        , "sei"
                                        , "zkevm"
                                        , "zksync"
                                        , "zora"
                                    ]\',
                                    "sector",
                                    "dex",
                                    \'["hosuke", "0xrob", "jeff-dude", "tomfutago"]\') }}')
}}

with dexs AS (
    {{
        log_decoded_enrich_dex_trades(
            base_trades = ref('dex_automated_base_trades')
            , filter = "project != 'curve'"
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)

Select * from dexs

