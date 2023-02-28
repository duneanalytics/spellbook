{{ config(
    schema = 'magiceden_polygon',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["polygon"]\'
                              "project",
                              "magiceden",
                              \'["springzh"]\') }}'
    )
}}

{% set nft_start_date = '2022-03-16' %}
{% set magic_eden_nonce = '10013141590000000000000000000000000000' %}

WITH  contract_list AS (
    SELECT DISTINCT erc721Token AS nft_contract_address
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC721OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '{{magic_eden_nonce}}'
    UNION ALL
    SELECT DISTINCT erc1155Token AS nft_contract_address
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC1155OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '{{magic_eden_nonce}}'
),

mints AS (
    SELECT 'mint' AS trade_category,
        block_time AS evt_block_time,
        block_number AS evt_block_number,
        tx_hash AS evt_tx_hash,
        CAST(NULL AS string) AS contract_address,
        evt_index,
        'Mint' AS evt_type,
        `to` AS buyer,
        CAST(NULL AS string) AS seller,
        contract_address AS nft_contract_address,
        token_id,
        amount AS number_of_items,
        token_standard,
        '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' AS currency_contract,
        CAST(0 AS DECIMAL(38,0)) AS amount_raw
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
    SELECT CASE when direction = 0 THEN 'buy' ELSE 'sell' END AS trade_category,
          evt_block_time,
          evt_block_number,
          evt_tx_hash,
          contract_address,
          evt_index,
          'Trade' AS evt_type,
          CASE when direction = 0 THEN taker ELSE maker END AS buyer,
          CASE when direction = 0 THEN maker ELSE taker END AS seller,
          erc721Token AS nft_contract_address,
          erc721TokenId AS token_id,
          1 AS number_of_items,
          'erc721' AS token_standard,
          CASE
               WHEN erc20Token in ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010') 
               THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
               ELSE erc20Token
          END AS currency_contract,
          erc20TokenAmount AS amount_raw
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC721OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '{{magic_eden_nonce}}'
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    UNION ALL

    SELECT CASE when direction = 0 THEN 'buy' ELSE 'sell' END AS trade_category,
          evt_block_time,
          evt_block_number,
          evt_tx_hash,
          contract_address,
          evt_index,
          'Trade' AS evt_type,
          CASE when direction = 0 THEN taker ELSE maker END AS buyer,
          CASE when direction = 0 THEN maker ELSE taker END AS seller,
          erc1155Token AS nft_contract_address,
          erc1155TokenId AS token_id,
          erc1155FillAmount AS number_of_items,
          'erc1155' AS token_standard,
          CASE
               WHEN erc20Token in ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010') 
               THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
               ELSE erc20Token
          END AS currency_contract,
          erc20FillAmount AS amount_raw
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC1155OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '{{magic_eden_nonce}}'
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

all_events AS (
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
        AND contract_address IN ( SELECT DISTINCT currency_contract FROM all_events) 
        {% if not is_incremental() %}
        AND minute >= '{{nft_start_date}}' 
        {% endif %}
        {% if is_incremental() %}
        AND minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
) 

SELECT
  'polygon' AS blockchain,
  'magiceden' AS project,
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
  CAST(NULL AS string) AS trade_category,
  buyer,
  seller,
  nft_contract_address,
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
  evt_tx_hash || '-' || evt_type  || '-' || evt_index || '-' || token_id  AS unique_trade_id
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
