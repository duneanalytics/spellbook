{{ config(
    schema = 'dex'
    , alias = 'roundtrip_trades'
    , materialized = 'view'
    , post_hook='{{ expose_spells(blockchains = \'["ethereum", "arbitrum", "optimism", "polygon", "bnb", "base", "celo", "avalanche_c", "unichain"]\',
                                      spell_type = "sector", 
                                      spell_name = "roundtrip_trades", 
                                      contributors = \'["Henrystats", "agaperste"]\') }}'
    )
}}


{% set dex_models = [
 ref('dex_bnb_roundtrip_trades')
, ref('dex_ethereum_roundtrip_trades')
] %}


SELECT *
FROM (
    {% for chain_model in dex_models %}
    SELECT
        blockchain
        , project
        , version
        , block_time
        , block_date
        , block_month
        , block_number
        , token_sold_address
        , token_bought_address
        , token_sold_symbol
        , token_bought_symbol
        , maker
        , taker
        , tx_hash
        , tx_from
        , tx_to
        , project_contract_address
        , pool_address
        , token_pair
        , token_sold_amount_raw
        , token_bought_amount_raw
        , token_sold_amount
        , token_bought_amount
        , amount_usd
        , evt_index
    FROM 
    {{ chain_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
