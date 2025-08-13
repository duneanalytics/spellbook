{{ config(
        alias = 'wash_trades',
        schema = 'dex',
        
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "gnosis", "optimism", "polygon", "celo", "base", "scroll", "zora", "blast", "fantom", "ronin", "linea", "nova", "abstract", "apechain"]\',
                                    "sector",
                                    "dex",
                                    \'["krishhh"]\') }}')
}}

{% set dex_wash_models = [
 ref('dex_bnb_wash_trades')
] %}

SELECT *
FROM (
    {% for dex_wash_model in dex_wash_models %}
    SELECT blockchain
    , project
    , version
    , block_time
    , block_date
    , block_month
    , block_number
    , tx_hash
    , tx_from
    , tx_to
    , token_bought_address
    , token_sold_address
    , token_bought_symbol
    , token_sold_symbol
    , token_bought_amount
    , token_sold_amount
    , token_bought_amount_raw
    , token_sold_amount_raw
    , amount_usd
    , project_contract_address
    , evt_index
    , token_pair
    , filter_1_same_wallet
    , filter_2_back_forth
    , filter_3_high_frequency
    , filter_4_circular_trading
    , filter_5_net_zero_pnl
    , is_wash_trade
    FROM {{ dex_wash_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    ) 