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

{% set nft_start_date = "2021-03-02" %}

WITH contract_list as (
    SELECT distinct erc721TokenAddress as nft_contract_address
    FROM {{ source ('aavegotchi_polygon', 'aavegotchi_diamond_evt_ERC721ExecutedListing') }}
    UNION ALL
    SELECT distinct erc1155TokenAddress as nft_contract_address
    FROM {{ source ('aavegotchi_polygon', 'aavegotchi_diamond_evt_ERC1155ExecutedListing') }}
),

mints as (
    SELECT 'mint' AS trade_category,
        block_time AS evt_block_time,
        block_number AS evt_block_number,
        tx_hash AS evt_tx_hash,
        CAST(NULL AS string) AS contract_address, -- We leave it NULL here and will get its value below by join from transactions table
        evt_index,
        'Mint' AS evt_type,
        `to` AS buyer,
        CAST(NULL AS string) AS seller,
        contract_address AS nft_contract_address,
        token_id,
        amount AS number_of_items,
        token_standard,
        '0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7' AS currency_contract, -- All sale are in GHST
        CAST(0 as DECIMAL(38,0)) AS amount_raw, -- It's hard to get the mint price. So handle it similar as in nftb_bnb_events
        CAST(NULL AS string) AS category,
        CAST(NULL AS string) AS executed_time
    FROM {{ ref('nft_polygon_transfers') }}
    WHERE contract_address IN ( SELECT nft_contract_address FROM contract_list )
        AND `from` = '0x0000000000000000000000000000000000000000'   -- mint
        {% if not is_incremental() %}
        AND block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

trades AS (
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
),

all_events as (
    SELECT * FROM mints
    UNION ALL
    SELECT * FROM trades
),

price_list AS (
    SELECT contract_address,
        minute,
        price,
        decimals,
        symbol
     FROM {{ source('prices', 'usd') }} p
     WHERE blockchain = 'polygon'
        AND contract_address IN ( SELECT DISTINCT currency_contract FROM all_events ) 
        {% if not is_incremental() %}
        AND minute >= '{{nft_start_date}}' 
        {% endif %}
        {% if is_incremental() %}
        AND minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
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
    a.evt_tx_hash AS tx_hash,
    date_trunc('day', a.evt_block_time) AS block_date,
    a.evt_block_time AS block_time,
    a.evt_block_number AS block_number,
    amount_raw / power(10, coalesce(p.decimals, gp.decimals)) * coalesce(p.price, gp.price) AS amount_usd,
    amount_raw / power(10, coalesce(p.decimals, gp.decimals)) AS amount_original,
    amount_raw,
    CASE WHEN p.symbol = 'WMATIC' THEN 'MATIC' ELSE coalesce(p.symbol, gp.symbol) END AS currency_symbol,
    coalesce(p.contract_address, gp.contract_address) AS currency_contract,
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
    2 * amount_raw / power(10, coalesce(p.decimals, gp.decimals)) / 100 AS platform_fee_amount,
    2 * amount_raw / power(10, coalesce(p.decimals, gp.decimals)) * coalesce(p.price, gp.price) / 100 AS platform_fee_amount_usd,
    CAST(2 AS DOUBLE) AS platform_fee_percentage, -- Treasury 0xd4151c984e6cf33e04ffaaf06c3374b2926ecc64 receive 2%
    0 AS royalty_fee_amount_raw,
    0 AS royalty_fee_amount,
    0 AS royalty_fee_amount_usd,
    0 AS royalty_fee_percentage,
    CAST(NULL AS double) AS royalty_fee_receive_address,
    CAST(NULL AS string) AS royalty_fee_currency_symbol,
    evt_tx_hash || '-' || evt_type || '-' || evt_index || '-' || token_id  AS unique_trade_id
FROM all_events AS a
INNER JOIN {{ source('polygon','transactions') }} t ON a.evt_block_number = t.block_number
    AND a.evt_tx_hash = t.hash
    {% if not is_incremental() %}
    AND t.block_time >= '{{nft_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
INNER JOIN ghst_initial_price gp ON true
LEFT JOIN price_list p ON p.contract_address = a.currency_contract AND p.minute = date_trunc('minute', a.evt_block_time)
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
