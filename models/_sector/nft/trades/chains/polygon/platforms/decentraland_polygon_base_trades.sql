{{ config(
    schema = 'decentraland_polygon',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
)}}

{% set nft_start_date = '2022-01-04' %}

WITH

src_data_enriched as (
    SELECT
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        assetId as nft_token_id,
        buyer,
        nftAddress as nft_contract_address,
        seller,
        totalPrice as price_raw
        -- '0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4' as token_address
    FROM
    {{ source('decentraland_polygon','MarketplaceV2_evt_OrderSuccessful') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
)

base_trades as (
SELECT
    'polygon' as blockchain,
    'decentraland' as project,
    'v1' as project_version,
    src.evt_block_time AS block_time,
    src.evt_block_number AS block_number,
    src.price_raw as price_raw,
    0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4 as currency_contract,
    nft_token_id,
    src.contract_address as project_contract_address,
    'secondary' as evt_type,
    uint256 '1' as nft_amount,
    'Buy' AS trade_category,
    src.buyer,
    src.seller,
    src.nftAddress as nft_contract_address,
    src.evt_tx_hash as tx_hash,
    CAST((COALESCE(src.price_raw, uint256 '0') * double '0.025') as uint256) as platform_fee_amount_raw,
    CAST(NULL as uint256) as royalty_fee_amount_raw,
    CAST(NULL as varbinary) as royalty_fee_address,
    CAST(NULL as varbinary) as platform_fee_address
FROM src_data_enriched src
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'polygon') }}
