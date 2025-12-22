{{ config(
    schema = 'dex'
    , alias = 'multihop_trades'
    , materialized = 'view'
    , post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "blast", "bnb", "celo", "ethereum", "gnosis", "ink", "linea", "mantle", "monad", "optimism", "plasma", "polygon", "scroll", "sonic", "unichain", "worldchain", "zksync", "zora"]\',
                                      spell_type = "sector", 
                                      spell_name = "dex", 
                                      contributors = \'["hubbymatic", "Henrystats", "agaperste"]\') }}'
    )
}}


{% set dex_models = [
ref('dex_arbitrum_multihop_trades')
, ref('dex_avalanche_c_multihop_trades')
, ref('dex_base_multihop_trades')
, ref('dex_blast_multihop_trades')
, ref('dex_bnb_multihop_trades')
, ref('dex_celo_multihop_trades')
, ref('dex_ethereum_multihop_trades')
, ref('dex_gnosis_multihop_trades')
, ref('dex_ink_multihop_trades')
, ref('dex_linea_multihop_trades')
, ref('dex_mantle_multihop_trades')
, ref('dex_monad_multihop_trades')
, ref('dex_optimism_multihop_trades')
, ref('dex_plasma_multihop_trades')
, ref('dex_polygon_multihop_trades')
, ref('dex_scroll_multihop_trades')
, ref('dex_sonic_multihop_trades')
, ref('dex_unichain_multihop_trades')
, ref('dex_worldchain_multihop_trades')
, ref('dex_zksync_multihop_trades')
, ref('dex_zora_multihop_trades')
] %}


SELECT *
FROM (
    {% for chain_model in dex_models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index 
        , token_bought_symbol
        , token_sold_symbol 
        , token_bought_address 
        , token_sold_address
        , token_pair
        , pool_address
        , total_trade_count
        , multihop_trade_count_pct
        , direct_trade_count
        , entry_trade_count
        , intermediate_trade_count
        , end_trade_count
        , total_trade_vol
        , multihop_trades_vol_pct
        , direct_trade_vol
        , entry_trade_vol
        , intermediate_trade_vol
        , end_trade_vol 
    FROM {{ chain_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)