{{ config(
    schema = 'fractal_polygon',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set nft_start_date = "TIMESTAMP '2022-12-30'" %}

WITH listing_detail AS (
    SELECT assetContract AS nft_contract_address,
        contract_address,
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        lister,
        listingId,
        from_hex(JSON_EXTRACT_SCALAR(listing,'$.tokenOwner')) AS tokenOwner,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.tokenId') AS UINT256) AS tokenId,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.startTime') AS TIMESTAMP) AS startTime,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.endTime') AS TIMESTAMP) AS endTime,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.quantity') AS UINT256)  AS quantity,
        from_hex(JSON_EXTRACT_SCALAR(listing,'$.currency')) AS currency,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.reservePricePerToken') AS UINT256)  AS reservePricePerToken,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.buyoutPricePerToken') AS UINT256) AS buyoutPricePerToken,
        JSON_EXTRACT_SCALAR(listing,'$.tokenType') AS tokenType,
        JSON_EXTRACT_SCALAR(listing,'$.listingType') AS listingType
    FROM {{ source ('fractal_polygon', 'Marketplace_evt_ListingAdded') }}
),

base_trades AS (
    SELECT
        'polygon' AS blockchain,
        'fractal' AS project,
        'v1' AS project_version,
        'buy' AS trade_category,
        'secondary' AS trade_type,
        s.evt_block_time as block_time,
        cast(date_trunc('day', s.evt_block_time) as date) as block_date,
        cast(date_trunc('month', s.evt_block_time) as date) as block_month,
        s.evt_block_number as block_number,
        s.evt_tx_hash as tx_hash,
        s.contract_address as project_contract_address,
        s.evt_index,
        s.buyer,
        s.lister AS seller,
        s.assetContract AS nft_contract_address,
        l.tokenId AS nft_token_id,
        s.quantityBought AS nft_amount,
        (CASE
            WHEN l.currency IN (0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, 0x0000000000000000000000000000000000001010) THEN 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
            ELSE l.currency
        END) AS currency_contract,
        cast(s.totalPricePaid as uint256) AS price_raw,
        coalesce(cast(s.totalPricePaid * double '0.03' as uint256), uint256 '0') as platform_fee_amount_raw,
        uint256 '0' AS royalty_fee_amount_raw,
        CAST(null AS varbinary) AS platform_fee_address,
        CAST(null AS varbinary) AS royalty_fee_address,
        s.evt_index as sub_tx_trade_id
    FROM {{ source ('fractal_polygon', 'Marketplace_evt_NewSale') }} s
    INNER JOIN listing_detail l ON s.listingId = l.listingId
    WHERE 1 = 1
        {% if not is_incremental() %}
        AND s.evt_block_time >= {{nft_start_date}}
        {% else %}
        AND {{incremental_predicate('s.evt_block_time')}}
        {% endif %}
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'polygon') }}
