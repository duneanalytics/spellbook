{{ config(
    schema = 'nft_old',
    alias = 'base_trades',
    materialized = 'view'
    )
}}


-- while we refactor more marketplace models, they should be removed here and added to the chain specific base_trades unions.
{% set nft_models = [
 ref('aavegotchi_polygon_events')
,ref('element_bnb_events')
,ref('element_avalanche_c_events')
,ref('element_polygon_events')
,ref('fractal_polygon_events')
,ref('liquidifty_bnb_events')
,ref('liquidifty_ethereum_events')
,ref('magiceden_polygon_events')
,ref('magiceden_solana_events')
,ref('nftb_bnb_events')
,ref('nftearth_optimism_events')
,ref('nftrade_bnb_events')
,ref('mooar_polygon_events')
,ref('oneplanet_polygon_events')
,ref('opensea_v3_arbitrum_events')
,ref('opensea_v4_arbitrum_events')
,ref('opensea_v1_ethereum_events')
,ref('opensea_v3_ethereum_events')
,ref('opensea_v4_ethereum_events')
,ref('opensea_v3_optimism_events')
,ref('opensea_v4_optimism_events')
,ref('opensea_v2_polygon_events')
,ref('opensea_v3_polygon_events')
,ref('opensea_v4_polygon_events')
,ref('opensea_solana_events')
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


-- we have to do some column wrangling here to convert the old schema to the new base_trades schema
SELECT * FROM  (
{% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        version as project_version,
        cast(date_trunc('day', block_time) as date) as block_date,
        cast(date_trunc('month', block_time) as date) as block_month,
        block_time,
        block_number,
        tx_hash,
        project_contract_address,
        trade_category,
        case when evt_type = 'Mint' then 'primary' else 'secondary' end as trade_type,
        buyer,
        seller,
        nft_contract_address,
        token_id as nft_token_id,
        number_of_items as nft_amount,
        amount_raw as price_raw,
        currency_contract,
        platform_fee_amount_raw,
        royalty_fee_amount_raw,
        cast(null as varbinary) as platform_fee_address,
        royalty_fee_receive_address as royalty_fee_address,
        tx_from,
        tx_to,
        cast(null as varbinary) as tx_data_marker,                                                  -- forwarc compatibility with aggregator marker matching
        row_number() over (partition by tx_hash order by unique_trade_id) as sub_tx_trade_id,       -- intermediate fix to fill this column
        row_number() over (partition by tx_hash, unique_trade_id order by tx_hash) as duplicates_rank
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
where duplicates_rank = 1
