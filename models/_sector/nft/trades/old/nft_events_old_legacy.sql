{{ config(
	tags=['legacy'],
	
    schema = 'nft',
    alias = alias('events_old', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_trade_id', 'blockchain'])
}}


{% set nft_models = [
 ref('aavegotchi_polygon_events_legacy')
,ref('element_bnb_events_legacy')
,ref('element_avalanche_c_events_legacy')
,ref('element_polygon_events_legacy')
,ref('fractal_polygon_events_legacy')
,ref('liquidifty_bnb_events_legacy')
,ref('liquidifty_ethereum_events_legacy')
,ref('magiceden_solana_events_legacy')
,ref('magiceden_polygon_events_legacy')
,ref('nftb_bnb_events_legacy')
,ref('nftearth_optimism_events_legacy')
,ref('nftrade_bnb_events_legacy')
,ref('oneplanet_polygon_events_legacy')
,ref('opensea_events_legacy')
,ref('pancakeswap_bnb_nft_events_legacy')
,ref('quix_seaport_optimism_events_legacy')
,ref('quix_v1_optimism_events_legacy')
,ref('quix_v2_optimism_events_legacy')
,ref('quix_v3_optimism_events_legacy')
,ref('quix_v4_optimism_events_legacy')
,ref('quix_v5_optimism_events_legacy')
,ref('rarible_polygon_events_legacy')
,ref('stealcam_arbitrum_events_legacy')
,ref('tofu_arbitrum_events_legacy')
,ref('tofu_bnb_events_legacy')
,ref('tofu_optimism_events_legacy')
,ref('tofu_polygon_events_legacy')
,ref('trove_ethereum_events_legacy')
,ref('trove_v1_arbitrum_events_legacy')
,ref('trove_v2_arbitrum_events_legacy')
,ref('zonic_optimism_events_legacy')
,ref('decentraland_polygon_events_legacy')
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
