{{ config(
    schema = 'dex'
    , alias = 'multihop_trades'
    , materialized = 'view'
    , post_hook='{{ expose_spells(blockchains = \'["ethereum", "arbitrum", "optimism", "polygon", "bnb", "base", "celo", "avalanche_c", "unichain"]\',
                                      spell_type = "sector", 
                                      spell_name = "dex", 
                                      contributors = \'["hubbymatic", "Henrystats", "agaperste"]\') }}'
    )
}}


{% set dex_models = [
ref('dex_bnb_multihop_trades')
, ref('dex_ethereum_multihop_trades')
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