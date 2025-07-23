{{ config(
    schema = 'uniswap'
    , alias = 'liquidity_events'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'event_type']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'[
                                      "ethereum","arbitrum","base","ink","blast","optimism","blast","bnb","zora","avalanche_c","unichain","worldchain"
                                    ]\',
                                    "project",
                                    "uniswap",
                                    \'["irishlatte19", "Henrystats"]\') }}')
}}

WITH dexes AS (
    {{
        enrich_dex_liq_with_prices(
            base_liquidity = ref('uniswap_base_liquidity_events')
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)
          SELECT
                 blockchain
                , project
                , version
                , block_month
                , block_date
                , block_time
                , block_number
                , id
                , tx_hash
                , evt_index
                , event_type
                , token0
                , token1
                , token0_symbol 
                , token1_symbol
                , amount0_raw
                , amount1_raw
                , amount0
                , amount1
                , amount0_usd
                , amount1_usd
           FROM
                dexes
          {% if is_incremental() %}
           WHERE
               {{ incremental_predicate('block_time') }}
          {% endif %}