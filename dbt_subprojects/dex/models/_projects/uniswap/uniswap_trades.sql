{{ config(
    schema = 'uniswap'
    , alias = 'trades'
    , materialized = 'view'
    , post_hook='{{ expose_spells(blockchains = \'["ethereum", "arbitrum", "optimism", "polygon", "bnb", "base", "celo", "avalanche_c", "unichain"]\',
                                      spell_type = "project", 
                                      spell_name = "uniswap", 
                                      contributors = \'["jeff-dude", "mtitus6", "Henrystats", "chrispearcx", "wuligy", "tomfutago", "phu"]\') }}'
    )
}}


{% set uniswap_models = [
ref('uniswap_arbitrum_trades')
, ref('uniswap_avalanche_c_trades')
, ref('uniswap_base_trades')
, ref('uniswap_blast_trades')
, ref('uniswap_bnb_trades')
, ref('uniswap_celo_trades')
, ref('uniswap_ethereum_trades')
, ref('uniswap_gnosis_trades')
, ref('uniswap_ink_trades')
, ref('uniswap_linea_trades')
, ref('uniswap_mantle_trades')
, ref('uniswap_monad_trades')
, ref('uniswap_optimism_trades')
, ref('uniswap_plasma_trades')
, ref('uniswap_polygon_trades')
, ref('uniswap_scroll_trades')
, ref('uniswap_sonic_trades')
, ref('uniswap_unichain_trades')
, ref('uniswap_worldchain_trades')
, ref('uniswap_zksync_trades')
, ref('uniswap_zora_trades')
] %}


SELECT *
FROM (
    {% for chain_model in uniswap_models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount_usd 
        , token_sold_amount_usd
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , pool_address 
        , token0_address
        , token1_address
        , token0_symbol 
        , token1_symbol 
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
        -- uni fee columns 
        , lp_fee_amount_usd
        , lp_fee_amount 
        , lp_fee_amount_raw
        , lp_fee -- fee tier denominated in % 
        -- hooks fee columns 
        , hooks_fee_amount_usd
        , hooks_fee_amount 
        , hooks_fee_amount_raw
        , hooks_fee 
        , hooks 
    FROM {{ chain_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

--note for future editor: angstrom hook fees are not included in the current iteration