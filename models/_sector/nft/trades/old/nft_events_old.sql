{{ config(
    schema = 'nft',
    alias ='events_old',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_trade_id', 'blockchain'])
}}


{% set nft_models = [
 ref('aavegotchi_polygon_events')
,ref('element_bnb_events')
,ref('element_avalanche_c_events')
,ref('element_polygon_events')
,ref('fractal_polygon_events')
,ref('liquidifty_bnb_events')
,ref('liquidifty_ethereum_events')
,ref('magiceden_solana_events')
,ref('magiceden_polygon_events')
,ref('nftb_bnb_events')
,ref('nftearth_optimism_events')
,ref('nftrade_bnb_events')
,ref('oneplanet_polygon_events')
,ref('opensea_events')
,ref('pancakeswap_bnb_nft_events')
,ref('quix_seaport_optimism_events')
,ref('quix_v1_optimism_events')
,ref('quix_v2_optimism_events')
,ref('quix_v3_optimism_events')
,ref('quix_v4_optimism_events')
,ref('quix_v5_optimism_events')
,ref('rarible_polygon_events')
,ref('stealcam_arbitrum_events')
,ref('tofu_arbitrum_events')
,ref('tofu_bnb_events')
,ref('tofu_optimism_events')
,ref('tofu_polygon_events')
,ref('trove_ethereum_events')
,ref('trove_v1_arbitrum_events')
,ref('trove_v2_arbitrum_events')
,ref('zonic_optimism_events')
,ref('decentraland_polygon_events')
] %}


SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        version,
        date_trunc('day', block_time)  as block_date,
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
        unique_trade_id,
        row_number() over (partition by unique_trade_id order by tx_hash) as duplicates_rank
    FROM {{ nft_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
WHERE duplicates_rank = 1
