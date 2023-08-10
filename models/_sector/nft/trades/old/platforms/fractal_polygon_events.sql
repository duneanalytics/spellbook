{{ config(
    schema = 'fractal_polygon',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
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
        CAST(JSON_EXTRACT_SCALAR(listing,'$.tokenOwner') AS VARBINARY) AS tokenOwner,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.tokenId') AS UINT256) AS tokenId,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.startTime') AS TIMESTAMP) AS startTime,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.endTime') AS TIMESTAMP) AS endTime,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.quantity') AS UINT256)  AS quantity,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.currency') AS VARBINARY) AS currency,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.reservePricePerToken') AS UINT256)  AS reservePricePerToken,
        CAST(JSON_EXTRACT_SCALAR(listing,'$.buyoutPricePerToken') AS UINT256) AS buyoutPricePerToken,
        JSON_EXTRACT_SCALAR(listing,'$.tokenType') AS tokenType,
        JSON_EXTRACT_SCALAR(listing,'$.listingType') AS listingType
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
            WHEN l.currency IN (0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, 0x0000000000000000000000000000000000001010) THEN 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
            ELSE l.currency
        END) AS currency_contract,
        l.currency as original_currency,
        s.totalPricePaid AS amount_raw
    FROM {{ source ('fractal_polygon', 'Marketplace_evt_NewSale') }} s
    INNER JOIN listing_detail l ON s.listingId = l.listingId
    WHERE 1 = 1
        {% if not is_incremental() %}
        AND s.evt_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND s.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

-- note: This logic will probably break down for multi-trade transactions.
trade_amount_detail as (
    SELECT e.block_number AS evt_block_number,
        e.tx_hash AS evt_tx_hash,
        cast(e.value AS uint256) as amount_raw,
        row_number() OVER (PARTITION BY e.tx_hash ORDER BY e.trace_address) AS item_index
    FROM {{ source('polygon', 'traces') }} e
    INNER JOIN trades t ON e.block_number = t.evt_block_number
        AND e.tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND e.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE t.original_currency IN (0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, 0x0000000000000000000000000000000000001010)
        AND cast(e.value as double) > 0
        AND cardinality(trace_address) > 0 -- exclude the main call record

    UNION ALL

    SELECT e.evt_block_number,
        e.evt_tx_hash,
        CAST(e.value as uint256) AS amount_raw,
        row_number() OVER (PARTITION BY e.evt_tx_hash ORDER BY e.evt_index) AS item_index
    FROM {{ source('erc20_polygon', 'evt_transfer') }} e
    INNER JOIN trades t ON e.evt_block_number = t.evt_block_number
        AND e.evt_tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.evt_block_time >= {{nft_start_date}}
        {% endif %}
        {% if is_incremental() %}
        AND e.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE t.original_currency NOT IN (0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, 0x0000000000000000000000000000000000001010)
),

trade_amount_summary as (
    SELECT evt_block_number,
        evt_tx_hash,
        sum(amount_raw) AS amount_raw,
        -- 1st is for platform fee, 2nd is for seller, 3rd is for royalty (no sample found so far)
        sum(case when item_index = 1 then amount_raw else cast(0 as uint256) end) AS platform_fee_amount_raw,
        sum(case when item_index = 3 then amount_raw else cast(0 as uint256) end) AS royalty_fee_amount_raw
    FROM trade_amount_detail
    GROUP BY 1, 2
)

SELECT
    'polygon' AS blockchain,
    'fractal' AS project,
    'v1' AS version,
    a.evt_tx_hash AS tx_hash,
    date_trunc('day', a.evt_block_time) AS block_date,
    a.evt_block_time AS block_time,
    a.evt_block_number AS block_number,
    coalesce(s.amount_raw,cast(0 as uint256)) / power(10, erc.decimals) * p.price AS amount_usd,
    coalesce(s.amount_raw,cast(0 as uint256)) / power(10, erc.decimals) AS amount_original,
    coalesce(s.amount_raw,cast(0 as uint256)) as amount_raw,
    erc.symbol AS currency_symbol,
    a.currency_contract,
    token_id,
    token_standard,
    a.contract_address AS project_contract_address,
    evt_type,
    CAST(NULL AS varchar) AS collection,
    CASE WHEN number_of_items = cast(1 as uint256) THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
    CAST(number_of_items AS uint256) AS number_of_items,
    a.trade_category,
    a.buyer,
    a.seller,
    a.nft_contract_address,
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    t."from" AS tx_from,
    t."to" AS tx_to,

    coalesce(s.platform_fee_amount_raw,cast(0 as uint256)) as platform_fee_amount_raw,
    CAST(coalesce(s.platform_fee_amount_raw,cast(0 as uint256)) / power(10, erc.decimals) AS double) AS platform_fee_amount,
    CAST(coalesce(s.platform_fee_amount_raw,cast(0 as uint256)) / power(10, erc.decimals) * p.price AS double) AS platform_fee_amount_usd,
    CAST(coalesce(s.platform_fee_amount_raw,cast(0 as uint256))  / s.amount_raw * 100 as double) as platform_fee_percentage,
    coalesce(s.royalty_fee_amount_raw,cast(0 AS uint256)) AS royalty_fee_amount_raw,
    CAST(coalesce(s.royalty_fee_amount_raw,cast(0 as uint256)) / power(10, erc.decimals) AS double) AS royalty_fee_amount,
    CAST(coalesce(s.royalty_fee_amount_raw,cast(0 as uint256)) / power(10, erc.decimals) * p.price AS double) AS royalty_fee_amount_usd,
    CAST(coalesce(s.royalty_fee_amount_raw,cast(0 as uint256)) / s.amount_raw * 100 AS double) AS royalty_fee_percentage,
    CAST(NULL AS varbinary) AS royalty_fee_receive_address,
    CAST(NULL AS varchar) AS royalty_fee_currency_symbol,
    cast(a.evt_tx_hash as varchar) || '-' || cast(a.evt_type as varchar) || '-' || cast(a.evt_index as varchar) || '-' || cast(a.token_id as varchar)  AS unique_trade_id
FROM trades a
INNER JOIN {{ source('polygon','transactions') }} t ON a.evt_block_number = t.block_number
    AND a.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= {{nft_start_date}}
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN trade_amount_summary s ON a.evt_block_number = s.evt_block_number AND a.evt_tx_hash = s.evt_tx_hash
LEFT JOIN {{ ref('tokens_erc20') }} erc ON erc.blockchain = 'polygon' AND erc.contract_address = a.currency_contract
LEFT JOIN {{ source('prices', 'usd') }} p ON p.blockchain = 'polygon'
    AND p.contract_address = a.currency_contract
    AND p.minute = date_trunc('minute', a.evt_block_time)
    {% if not is_incremental() %}
    AND p.minute >= {{nft_start_date}}
    {% endif %}
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t."to"
