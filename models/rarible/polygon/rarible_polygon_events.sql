{{ config(
    schema = 'rarible_polygon',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["polygon"]\'
                              "project",
                              "rarible",
                              \'["springzh"]\') }}'
    )
}}

{% set nft_start_date = "2022-02-23" %}

WITH trades AS (
    select 'buy' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
        rightMaker AS buyer,
        leftMaker AS seller,
        '0x' || right(substring(leftAsset:data, 3, 64), 40) AS nft_contract_address,
        CAST(bytea2numeric_v3(substr(leftAsset:data, 3 + 64, 64)) AS string) AS token_id,
        newRightFill AS number_of_items,
        CASE WHEN leftAsset:assetClass = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN rightAsset:assetClass = '0xaaaebeba' THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE '0x' || right(substring(rightAsset:data, 3, 64), 40)
        END AS currency_contract,
        newLeftFill AS amount_raw
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE rightAsset:assetClass in ('0xaaaebeba', '0x8ae85d84') -- 0xaaaebeba: MATIC; 0x8ae85d84: ERC20 TOKEN
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    UNION ALL

    select 'sell' AS trade_category,
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        contract_address,
        evt_index,
        'Trade' AS evt_type,
        leftMaker AS buyer,
        rightMaker AS seller,
        '0x' || right(substring(rightAsset:data, 3, 64), 40) AS nft_contract_address,
        CAST(bytea2numeric_v3(substr(rightAsset:data, 3 + 64, 64)) AS string) AS token_id,
        newLeftFill AS number_of_items,
        CASE WHEN rightAsset:assetClass = '0x73ad2146' THEN 'erc721' ELSE 'erc1155' END AS token_standard, -- 0x73ad2146: erc721; 0x973bb640: erc1155
        CASE WHEN leftAsset:assetClass = '0xaaaebeba' THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
            ELSE '0x' || right(substring(leftAsset:data, 3, 64), 40)
        END AS currency_contract,
        newRightFill AS amount_raw
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE leftAsset:assetClass in ('0xaaaebeba', '0x8ae85d84') -- 0xaaaebeba: MATIC; 0x8ae85d84: ERC20 TOKEN
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

contract_list as (
    SELECT distinct nft_contract_address
    FROM trades
),

mints AS (
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
        CAST(0 as DECIMAL(38,0)) AS amount_raw -- It's hard to get the mint price. So handle it similar as in nftb_bnb_events
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
        AND contract_address IN ( SELECT DISTINCT currency_contract FROM trades) 
        {% if not is_incremental() %}
        AND minute >= '{{nft_start_date}}' 
        {% endif %}
        {% if is_incremental() %}
        AND minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
) 

SELECT
  'polygon' AS blockchain,
  'rarible' AS project,
  'v1' AS version,
  a.evt_tx_hash AS tx_hash,
  date_trunc('day', a.evt_block_time) AS block_date,
  a.evt_block_time AS block_time,
  a.evt_block_number AS block_number,
  amount_raw / power(10, p.decimals) * p.price AS amount_usd,
  amount_raw / power(10, p.decimals) AS amount_original,
  amount_raw,
  CASE WHEN p.symbol = 'WMATIC' THEN 'MATIC' ELSE p.symbol END AS currency_symbol,
  p.contract_address AS currency_contract,
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
  0 AS royalty_fee_amount,
  0 AS royalty_fee_amount_usd,
  0 AS royalty_fee_percentage,
  CAST(NULL AS double) AS royalty_fee_receive_address,
  CAST(NULL AS string)  AS royalty_fee_currency_symbol,
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
LEFT JOIN price_list p ON p.contract_address = a.currency_contract AND p.minute = date_trunc('minute', a.evt_block_time)
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
