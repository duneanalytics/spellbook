{{ config(
        alias ='fees',
        post_hook='{{ expose_spells(\'["ethereum","solana","bnb","optimism","arbitrum","polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke","0xRob", "hildobby"]\') }}')
}}


{% set nft_models = [
 ref('aavegotchi_polygon_fees')
,ref('archipelago_ethereum_fees')
,ref('blur_ethereum_fees')
,ref('element_fees')
,ref('foundation_ethereum_fees')
,ref('fractal_polygon_fees')
,ref('looksrare_ethereum_fees')
,ref('magiceden_fees')
,ref('oneplanet_polygon_fees')
,ref('opensea_fees')
,ref('sudoswap_ethereum_fees')
,ref('superrare_ethereum_fees')
,ref('x2y2_ethereum_fees')
,ref('zora_ethereum_fees')
,ref('pancakeswap_bnb_nft_fees')
,ref('quix_optimism_fees')
,ref('rarible_polygon_fees')
,ref('nftrade_bnb_fees')
,ref('zonic_optimism_fees')
,ref('nftb_bnb_fees')
,ref('tofu_fees')
,ref('nftearth_optimism_fees')
,ref('stealcam_arbitrum_fees')
] %}


SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        version,
        block_time,
        token_id,
        collection,
        platform_fee_amount_raw,
        platform_fee_amount,
        platform_fee_amount_usd,
        platform_fee_percentage,
        royalty_fee_amount_raw,
        royalty_fee_amount,
        royalty_fee_amount_usd,
        royalty_fee_percentage,
        royalty_fee_receive_address,
        royalty_fee_currency_symbol,
        token_standard,
        trade_type,
        number_of_items,
        trade_category,
        evt_type,
        seller,
        buyer,
        nft_contract_address,
        project_contract_address,
        aggregator_name,
        aggregator_address,
        tx_hash,
        block_number,
        tx_from,
        tx_to,
        unique_trade_id
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
