{{ config(
    schema = 'fractal_polygon',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["polygon"]\'
                              "project",
                              "fractal",
                              \'["springzh"]\') }}'
    )
}}

-- Initial date of ListingAdded
{% set nft_start_date = "2022-12-30" %}

WITH listing_detail AS (
    SELECT assetContract AS nft_contract_address,
        contract_address,
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        lister,
        listingId,
        listing:tokenOwner AS tokenOwner,
        listing:tokenId AS tokenId,
        listing:startTime AS startTime,
        listing:endTime AS endTime,
        listing:quantity AS quantity,
        listing:currency AS currency,
        listing:reservePricePerToken AS reservePricePerToken,
        listing:buyoutPricePerToken AS buyoutPricePerToken,
        listing:tokenType AS tokenType,
        listing:listingType AS listingType
    FROM {{ source ('fractal_polygon', 'Marketplace_evt_ListingAdded') }}
),

nft_order AS (
    SELECT 'buy' AS trade_category,
        s.evt_block_time,
        s.evt_block_number,
        s.evt_tx_hash,
        s.contract_address,
        s.evt_index,
        s.buyer,
        s.lister AS seller,
        s.assetContract AS nft_contract_address,
        '' AS collection,
        l.tokenId AS token_id,
        s.quantityBought AS number_of_items,
        'erc721' AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' AS currency_contract, -- All sale are in MATIC
        s.totalPricePaid AS amount_raw
    FROM {{ source ('fractal_polygon', 'Marketplace_evt_NewSale') }} s
    INNER JOIN listing_detail l ON s.listingId = l.listingId
    WHERE 1 = 1
        AND s.evt_block_time >= '{{nft_start_date}}'
),

price_list AS (
    SELECT contract_address,
        minute,
        price,
        decimals,
        symbol
     FROM {{ source('prices', 'usd') }} p
     WHERE blockchain = 'polygon'
        AND contract_address IN ( SELECT DISTINCT currency_contract FROM nft_order) 
        AND minute >= '{{nft_start_date}}' 
) 

SELECT
    'polygon' AS blockchain,
    'fractal' AS project,
    'v1' AS version,
    o.evt_tx_hash AS tx_hash,
    date_trunc('day', o.evt_block_time) AS block_date,
    o.evt_block_time AS block_time,
    o.evt_block_number AS block_number,
    amount_raw / power(10, p.decimals) * p.price AS amount_usd,
    amount_raw / power(10, p.decimals) AS amount_original,
    amount_raw,
    CASE WHEN p.symbol = 'WMATIC' THEN 'MATIC' ELSE p.symbol END AS currency_symbol,
    p.contract_address AS currency_contract,
    token_id,
    token_standard,
    o.contract_address AS project_contract_address,
    'Trade' AS evt_type,
    NULL::string AS collection,
    CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
    number_of_items,
    o.trade_category,
    o.buyer,
    o.seller,
    o.nft_contract_address,
    NULL::string AS aggregator_name,
    NULL::string AS aggregator_address,
    t.`from` AS tx_from,
    t.`to` AS tx_to,
    2 * amount_raw / 100 AS platform_fee_amount_raw,
    2 * amount_raw / power(10, p.decimals) / 100 AS platform_fee_amount,
    2 * amount_raw / power(10, p.decimals) * p.price / 100 AS platform_fee_amount_usd,
    CAST(2 AS DOUBLE) AS platform_fee_percentage,
    0 royalty_fee_amount,
    0 AS royalty_fee_amount_usd,
    0 AS royalty_fee_percentage,
    NULL::double AS royalty_fee_receive_address,
    NULL::string AS royalty_fee_currency_symbol,
    evt_tx_hash || '-' || evt_index || '-' || token_id  AS unique_trade_id
FROM nft_order o
INNER JOIN {{ source('polygon','transactions') }} t ON o.evt_block_number = t.block_number
    AND o.evt_tx_hash = t.hash
    AND t.block_time >= '{{nft_start_date}}'
LEFT JOIN price_list p ON p.contract_address = o.currency_contract AND p.minute = date_trunc('minute', o.evt_block_time)
WHERE 1 = 1
    AND o.evt_block_time >= '{{nft_start_date}}'
