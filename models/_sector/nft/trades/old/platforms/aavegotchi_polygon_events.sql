{{ config(
    schema = 'aavegotchi_polygon',
    alias = alias('events'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
    )
}}

{% set nft_start_date = "2021-03-02" %}

WITH trades AS (
    SELECT 'buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
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
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    UNION ALL

    SELECT 'buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
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
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)


SELECT
    'polygon' AS blockchain,
    'aavegotchi' AS project,
    'v1' AS version,
    a.evt_tx_hash AS tx_hash,
    date_trunc('day', a.evt_block_time) AS block_date,
    a.evt_block_time AS block_time,
    a.evt_block_number AS block_number,
    CAST(amount_raw / power(10, erc.decimals) * p.price as double) AS amount_usd,
    CAST(amount_raw / power(10, erc.decimals) AS double) AS amount_original,
    CAST(amount_raw as decimal(38, 0)) AS amount_raw,
    erc.symbol AS currency_symbol,
    a.currency_contract,
    token_id,
    token_standard,
    a.contract_address AS project_contract_address,
    evt_type,
    CAST(NULL AS VARCHAR(100)) AS collection,
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
    CAST(2 * amount_raw / 100 AS double) AS platform_fee_amount_raw,
    CAST(2 * amount_raw / power(10, erc.decimals) / 100 AS double) AS platform_fee_amount,
    CAST(2 * amount_raw / power(10, erc.decimals) * p.price / 100 AS double) AS platform_fee_amount_usd,
    CAST(2 AS double) AS platform_fee_percentage, -- Treasury 0xd4151c984e6cf33e04ffaaf06c3374b2926ecc64 receive 2%
    CAST(0 AS double) AS royalty_fee_amount_raw,
    CAST(0 AS double) AS royalty_fee_amount,
    CAST(0 AS double) AS royalty_fee_amount_usd,
    CAST(0 AS double) AS royalty_fee_percentage,
    CAST(NULL AS VARCHAR(100)) AS royalty_fee_receive_address,
    CAST(NULL AS VARCHAR(100)) AS royalty_fee_currency_symbol,
    evt_tx_hash || '-' || evt_type || '-' || evt_index || '-' || token_id AS unique_trade_id
FROM trades AS a
INNER JOIN {{ source('polygon','transactions') }} t ON a.evt_block_number = t.block_number
    AND a.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc
    ON erc.blockchain = 'polygon'
    AND erc.contract_address = a.currency_contract
-- There is no price data for GHST token before 2022-10-27, these trades won't have usd information
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.blockchain = 'polygon'
    AND p.contract_address = a.currency_contract
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    AND p.minute = date_trunc('minute', a.evt_block_time)
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
