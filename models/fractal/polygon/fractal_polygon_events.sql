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
        (CASE
            WHEN l.currency IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010') THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE l.currency
        END) AS currency_contract,
        l.currency as original_currency,
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
),

fees as (
    -- All trades are paid in native token. So we get the fees from traces
    SELECT e.block_number AS evt_block_number,
        e.tx_hash AS evt_tx_hash,
        CAST(e.value as double) AS platform_fee_amount_raw
    FROM {{ source('polygon', 'traces') }} e
    INNER JOIN trades t ON e.block_number = t.evt_block_number
        AND e.tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND e.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE e.`to` = '0x863087f3ad1a5a1d7e78160ceee4676afd469306'
        AND t.original_currency IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010') 
)

SELECT
    'polygon' AS blockchain,
    'fractal' AS project,
    'v1' AS version,
    a.evt_tx_hash AS tx_hash,
    date_trunc('day', a.evt_block_time) AS block_date,
    a.evt_block_time AS block_time,
    a.evt_block_number AS block_number,
    amount_raw / power(10, erc.decimals) * p.price AS amount_usd,
    amount_raw / power(10, erc.decimals) AS amount_original,
    amount_raw,
    erc.symbol AS currency_symbol,
    a.currency_contract,
    token_id,
    token_standard,
    a.contract_address AS project_contract_address,
    evt_type,
    CAST(NULL AS string) AS collection,
    CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
    CAST(number_of_items AS decimal(38,0)) AS number_of_items,
    a.trade_category,
    a.buyer,
    a.seller,
    a.nft_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    t.`from` AS tx_from,
    t.`to` AS tx_to,
    f.platform_fee_amount_raw,
    CAST(f.platform_fee_amount_raw / power(10, erc.decimals) AS double) AS platform_fee_amount,
    CAST(f.platform_fee_amount_raw / power(10, erc.decimals) * p.price AS double) AS platform_fee_amount_usd,
    CASE WHEN t.value > 0 THEN CAST(f.platform_fee_amount_raw / t.value * 100 as double)
        ELSE CAST(f.platform_fee_amount_raw / (coalesce(a.amount_raw, 0) + coalesce(f.platform_fee_amount_raw, 0)) * 100 AS double)
    END AS platform_fee_percentage,
    CAST(0 AS double) AS royalty_fee_amount_raw,
    CAST(0 AS double) AS royalty_fee_amount,
    CAST(0 AS double) AS royalty_fee_amount_usd,
    CAST(0 AS double) AS royalty_fee_percentage,
    CAST(NULL AS double) AS royalty_fee_receive_address,
    CAST(NULL AS string) AS royalty_fee_currency_symbol,
    a.evt_tx_hash || '-' || a.evt_type || '-' || a.evt_index || '-' || a.token_id  AS unique_trade_id
FROM trades a
INNER JOIN {{ source('polygon','transactions') }} t ON a.evt_block_number = t.block_number
    AND a.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN fees f ON a.evt_block_number = f.evt_block_number AND a.evt_tx_hash = f.evt_tx_hash
LEFT JOIN tokens.erc20 erc ON erc.blockchain = 'polygon' AND erc.contract_address = a.currency_contract
LEFT JOIN {{ source('prices', 'usd') }} p ON p.blockchain = 'polygon'
    AND p.contract_address = a.currency_contract
    AND p.minute = date_trunc('minute', a.evt_block_time)
    {% if not is_incremental() %}
    AND p.minute >= '{{nft_start_date}}' 
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
