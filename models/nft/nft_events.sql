{{ config(
    alias ='events',
    post_hook='{{ expose_spells(\'["ethereum","solana","bnb","optimism","arbitrum","polygon"]\',
                    "sector",
                    "nft",
                    \'["soispoke","0xRob", "hildobby"]\') }}')
}}

{% set nft_models = [
 ref('aavegotchi_polygon_events')
,ref('archipelago_ethereum_events')
,ref('blur_ethereum_events')
,ref('cryptopunks_ethereum_events')
,ref('element_events')
,ref('foundation_ethereum_events')
,ref('fractal_polygon_events')
,ref('looksrare_ethereum_events')
,ref('magiceden_events')
,ref('opensea_events')
,ref('sudoswap_ethereum_events')
,ref('superrare_ethereum_events')
,ref('x2y2_ethereum_events')
,ref('zora_ethereum_events')
,ref('oneplanet_polygon_events')
,ref('pancakeswap_bnb_nft_events')
,ref('tofu_events')
,ref('quix_optimism_events')
,ref('nftrade_bnb_events')
,ref('zonic_optimism_events')
,ref('nftb_bnb_events')
,ref('nftearth_optimism_events')
,ref('rarible_polygon_events')
,ref('stealcam_arbitrum_events')
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
        amount_usd,
        token_standard,
        trade_type,
        number_of_items,
        trade_category,
        evt_type,
        seller,
        buyer,
        amount_original,
        amount_raw,
        currency_symbol,
        currency_contract,
        nft_contract_address,
        project_contract_address,
        aggregator_name,
        aggregator_address,
        tx_hash,
        block_number,
        tx_from,
        tx_to,
        platform_fee_amount_raw,
        platform_fee_amount,
        platform_fee_amount_usd,
        platform_fee_percentage,
        royalty_fee_receive_address,
        royalty_fee_currency_symbol,
        royalty_fee_amount_raw,
        royalty_fee_amount,
        royalty_fee_amount_usd,
        royalty_fee_percentage,
        unique_trade_id
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
