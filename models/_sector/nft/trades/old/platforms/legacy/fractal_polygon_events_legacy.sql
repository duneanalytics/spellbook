{{ config(
	tags=['legacy'],
	
    schema = 'fractal_polygon',
    alias = alias('events', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
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

-- note: This logic will probably break down for multi-trade transactions.
trade_amount_detail as (
    SELECT e.block_number AS evt_block_number,
        e.tx_hash AS evt_tx_hash,
        cast(e.value AS double) as amount_raw,
        row_number() OVER (PARTITION BY e.tx_hash ORDER BY e.trace_address) AS item_index
    FROM {{ source('polygon', 'traces') }} e
    INNER JOIN trades t ON e.block_number = t.evt_block_number
        AND e.tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND e.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE t.original_currency IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010')
        AND cast(e.value as double) > 0
        AND cardinality(trace_address) > 0 -- exclude the main call record

    UNION ALL

    SELECT e.evt_block_number,
        e.evt_tx_hash,
        CAST(e.value as double) AS amount_raw,
        row_number() OVER (PARTITION BY e.evt_tx_hash ORDER BY e.evt_index) AS item_index
    FROM {{ source('erc20_polygon', 'evt_transfer') }} e
    INNER JOIN trades t ON e.evt_block_number = t.evt_block_number
        AND e.evt_tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND e.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE t.original_currency NOT IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010')
),

trade_amount_summary as (
    SELECT evt_block_number,
        evt_tx_hash,
        sum(amount_raw) AS amount_raw,
        -- 1st is for platform fee, 2nd is for seller, 3rd is for royalty (no sample found so far)
        sum(case when item_index = 1 then amount_raw else 0 end) AS platform_fee_amount_raw,
        sum(case when item_index = 3 then amount_raw else 0 end) AS royalty_fee_amount_raw
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
    coalesce(s.amount_raw,0) / power(10, erc.decimals) * p.price AS amount_usd,
    coalesce(s.amount_raw,0) / power(10, erc.decimals) AS amount_original,
    coalesce(s.amount_raw,0) as amount_raw,
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

    coalesce(s.platform_fee_amount_raw,0) as platform_fee_amount_raw,
    CAST(coalesce(s.platform_fee_amount_raw,0) / power(10, erc.decimals) AS double) AS platform_fee_amount,
    CAST(coalesce(s.platform_fee_amount_raw,0) / power(10, erc.decimals) * p.price AS double) AS platform_fee_amount_usd,
    CAST(coalesce(s.platform_fee_amount_raw,0)  / s.amount_raw * 100 as double) as platform_fee_percentage,
    CAST(coalesce(s.royalty_fee_amount_raw,0) AS double) AS royalty_fee_amount_raw,
    CAST(coalesce(s.royalty_fee_amount_raw,0) / power(10, erc.decimals) AS double) AS royalty_fee_amount,
    CAST(coalesce(s.royalty_fee_amount_raw,0) / power(10, erc.decimals) * p.price AS double) AS royalty_fee_amount_usd,
    CAST(coalesce(s.royalty_fee_amount_raw,0) / s.amount_raw * 100 AS double) AS royalty_fee_percentage,
    CAST(NULL AS varchar(5)) AS royalty_fee_receive_address,
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
LEFT JOIN trade_amount_summary s ON a.evt_block_number = s.evt_block_number AND a.evt_tx_hash = s.evt_tx_hash
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
LEFT JOIN {{ ref('nft_aggregators_legacy') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
