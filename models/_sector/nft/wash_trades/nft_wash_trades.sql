{{ config(
        alias = 'wash_trades',
        schema = 'nft',
        
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "gnosis", "optimism", "polygon", "celo", "zksync"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}')
}}

{% set nft_wash_models = [
 ref('nft_arbitrum_wash_trades')
, ref('nft_avalanche_c_wash_trades')
, ref('nft_bnb_wash_trades')
, ref('nft_ethereum_wash_trades')
, ref('nft_gnosis_wash_trades')
, ref('nft_optimism_wash_trades')
, ref('nft_polygon_wash_trades')
, ref('nft_celo_wash_trades')
, ref('nft_zksync_wash_trades')
] %}

SELECT *
FROM (
    {% for nft_wash_model in nft_wash_models %}
    SELECT blockchain
    , project
    , version
    , nft_contract_address
    , token_id
    , token_standard
    , trade_category
    , buyer
    , seller
    , project_contract_address
    , aggregator_name
    , aggregator_address
    , tx_from
    , tx_to
    , block_time
    , block_date
    , block_number
    , tx_hash
    , unique_trade_id
    , buyer_first_funded_by
    , seller_first_funded_by
    , filter_1_same_buyer_seller
    , filter_2_back_and_forth_trade
    , filter_3_bought_or_sold_3x
    , filter_4_first_funded_by_same_wallet
    , filter_5_flashloan
    , is_wash_trade
    FROM {{ nft_wash_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
