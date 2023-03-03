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

trades AS (
    SELECT 'buy' AS trade_category,
        s.evt_block_time,
        s.evt_block_number,
        s.evt_tx_hash,
        s.contract_address,
        s.evt_index,
        'Trade' AS evt_type,
        s.buyer,
        s.lister AS seller,
        s.assetContract AS nft_contract_address,
        l.tokenId AS token_id,
        s.quantityBought AS number_of_items,
        'erc721' AS token_standard, -- All of erc721 type
        '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' AS currency_contract, -- All sale are in MATIC
        s.totalPricePaid AS amount_raw
    FROM {{ source ('fractal_polygon', 'Marketplace_evt_NewSale') }} s
    INNER JOIN listing_detail l ON s.listingId = l.listingId
    WHERE 1 = 1
        {% if not is_incremental() %}
        AND s.evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND s.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT
    'polygon' AS blockchain,
    'fractal' AS project,
    'v1' AS version,
    a.evt_tx_hash AS tx_hash,
    date_trunc('day', a.evt_block_time) AS block_date,
    a.evt_block_time AS block_time,
    a.evt_block_number AS block_number,
    amount_raw / power(10, p.decimals) * p.price AS amount_usd,
    amount_raw / power(10, p.decimals) AS amount_original,
    amount_raw,
    p.symbol AS currency_symbol,
    a.currency_contract,
    token_id,
    token_standard,
    coalesce(a.contract_address, t.`to`) AS project_contract_address,
    evt_type,
    CAST(NULL AS string) AS collection,
    CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
    number_of_items,
    a.trade_category,
    a.buyer,
    a.seller,
    a.nft_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    t.`from` AS tx_from,
    t.`to` AS tx_to,
    2 * amount_raw / 100 AS platform_fee_amount_raw,
    2 * amount_raw / power(10, p.decimals) / 100 AS platform_fee_amount,
    2 * amount_raw / power(10, p.decimals) * p.price / 100 AS platform_fee_amount_usd,
    CAST(2 AS DOUBLE) AS platform_fee_percentage,
    0 AS royalty_fee_amount_raw,
    0 AS royalty_fee_amount,
    0 AS royalty_fee_amount_usd,
    0 AS royalty_fee_percentage,
    CAST(NULL AS double) AS royalty_fee_receive_address,
    CAST(NULL AS string) AS royalty_fee_currency_symbol,
    evt_tx_hash || '-' || evt_type || '-' || evt_index || '-' || token_id  AS unique_trade_id
FROM all_events a
INNER JOIN {{ source('polygon','transactions') }} t ON a.evt_block_number = t.block_number
    AND a.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.blockchain = 'polygon'
    {% if not is_incremental() %}
    AND p.minute >= '{{nft_start_date}}'
    {% endif %}
    AND p.contract_address = a.currency_contract
    AND p.minute = date_trunc('minute', a.evt_block_time)
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
