{{ config(
    schema = 'mass_decoded_dex_trades'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "blast", "bnb", "celo", "ethereum", "fantom", "gnosis", "linea", "optimism", "polygon", "scroll", "zkevm", "zksync", "zora"]\',
                                    "sector",
                                    "dex",
                                    \'["hosuke", "0xrob", "jeff-dude", "tomfutago"]\') }}')
}}


with dexs AS (
    {{
        enrich_dex_trades(
            base_trades = ref("mass_decoded_dex_trades_cross_chain")
            , filter = "1=1"
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)

Select * from dexs