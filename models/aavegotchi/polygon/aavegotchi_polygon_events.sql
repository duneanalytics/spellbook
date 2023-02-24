{{ config(
    schema = 'aavegotchi_polygon',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["polygon"]\'
                              "project",
                              "aavegotchi",
                              \'["springzh"]\') }}'
    )
}}

-- Initial date of ListingAdded
{% set nft_start_date = "2021-03-02" %}

WITH nft_order AS (
    SELECT 'buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        buyer,
        seller,
        erc721TokenAddress AS nft_contract_address,
        erc721TokenId AS token_id,
        cast(1 as bigint) AS number_of_items,
        'erc721' AS token_standard,
        '0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7' AS currency_contract, -- All sale are in GHST
        priceInWei AS amount_raw,
        category,
        `time` AS executed_time
    FROM {{ source ('aavegotchi_polygon', 'aavegotchi_diamond_evt_ERC721ExecutedListing') }}
    WHERE 1 = 1
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}

    UNION ALL

    SELECT 'buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        buyer,
        seller,
        erc1155TokenAddress AS nft_contract_address,
        erc1155TypeId AS token_id,
        cast(_quantity as bigint) AS number_of_items,
        'erc1155' AS token_standard,
        '0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7' AS currency_contract, -- All sale are in GHST
        priceInWei AS amount_raw,
        category,
        `time` AS executed_time
    FROM {{ source ('aavegotchi_polygon', 'aavegotchi_diamond_evt_ERC1155ExecutedListing') }}
    WHERE 1 = 1
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
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
),

-- There is no price data for GHST token before 2022-10-27, so we use the first record value for missing data.
ghst_initial_price as (
    SELECT contract_address,
        minute,
        price,
        decimals,
        symbol
    FROM {{ source('prices', 'usd') }} p
    WHERE blockchain = 'polygon'
       AND contract_address = '0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7'
    ORDER BY minute
    LIMIT 1 
)

SELECT
    'polygon' AS blockchain,
    'aavegotchi' AS project,
    'v1' AS version,
    o.evt_tx_hash AS tx_hash,
    date_trunc('day', o.evt_block_time) AS block_date,
    o.evt_block_time AS block_time,
    o.evt_block_number AS block_number,
    amount_raw / power(10, coalesce(p.decimals, gp.decimals)) * coalesce(p.price, gp.price) AS amount_usd,
    amount_raw / power(10, coalesce(p.decimals, gp.decimals)) AS amount_original,
    amount_raw,
    CASE WHEN p.symbol = 'WMATIC' THEN 'MATIC' ELSE coalesce(p.symbol, gp.symbol) END AS currency_symbol,
    coalesce(p.contract_address, gp.contract_address) AS currency_contract,
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
    agg.name AS aggregator_name,
    agg.contract_address AS aggregator_address,
    t.`from` AS tx_from,
    t.`to` AS tx_to,
    2 * amount_raw / 100 AS platform_fee_amount_raw,
    2 * amount_raw / power(10, coalesce(p.decimals, gp.decimals)) / 100 AS platform_fee_amount,
    2 * amount_raw / power(10, coalesce(p.decimals, gp.decimals)) * coalesce(p.price, gp.price) / 100 AS platform_fee_amount_usd,
    CAST(2 AS DOUBLE) AS platform_fee_percentage, -- Treasury 0xd4151c984e6cf33e04ffaaf06c3374b2926ecc64 receive 2%
    0 AS royalty_fee_amount_raw,
    0 AS royalty_fee_amount,
    0 AS royalty_fee_amount_usd,
    0 AS royalty_fee_percentage,
    NULL::double AS royalty_fee_receive_address,
    NULL::string AS royalty_fee_currency_symbol,
    evt_tx_hash || '-' || evt_index || '-' || token_id  AS unique_trade_id
FROM nft_order o
INNER JOIN {{ source('polygon','transactions') }} t ON o.evt_block_number = t.block_number
    AND o.evt_tx_hash = t.hash
    AND t.block_time >= '{{nft_start_date}}'
INNER JOIN ghst_initial_price gp ON true
LEFT JOIN price_list p ON p.contract_address = o.currency_contract AND p.minute = date_trunc('minute', o.evt_block_time)
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
WHERE 1 = 1
    AND o.evt_block_time >= '{{nft_start_date}}'
