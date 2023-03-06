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

WITH trades AS (
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
          erc20TokenAmount AS amount_raw,
          erc20Token as original_erc20_token
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
          erc20FillAmount AS amount_raw,
          erc20Token as original_erc20_token
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC1155OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '{{magic_eden_nonce}}'
        {% if not is_incremental() %}
        AND evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

fees as (
    SELECT e.evt_block_number,
        e.evt_tx_hash,
        CAST(e.value as double) AS platform_fee_amount_raw
    FROM {{ source('erc20_polygon', 'evt_transfer') }} e
    INNER JOIN trades t ON e.evt_block_number = t.evt_block_number
        AND e.evt_tx_hash = t.evt_tx_hash
        {% if not is_incremental() %}
        AND e.evt_block_time >= '{{nft_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND e.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE e.`to` IN ('0xca9337244b5f04cb946391bc8b8a980e988f9a6a',
                    '0x58a24fa9ae8847cbcf245dd2ef7fcef205927af1',
                    '0x9210f8a17110f939cf223e42e1eaf1553c4ba2c6')
        AND t.original_erc20_token NOT IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010') 

    UNION ALL

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
    WHERE e.`to` IN ('0xca9337244b5f04cb946391bc8b8a980e988f9a6a',
                    '0x58a24fa9ae8847cbcf245dd2ef7fcef205927af1',
                    '0x9210f8a17110f939cf223e42e1eaf1553c4ba2c6')
        AND t.original_erc20_token IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010') 
),

price_list AS (
    SELECT contract_address,
          minute,
          price,
          decimals,
          symbol
     FROM {{ source('prices', 'usd') }} p
     WHERE blockchain = 'polygon'
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
  amount_raw / power(10, erc.decimals) * p.price AS amount_usd,
  amount_raw / power(10, erc.decimals) AS amount_original,
  CAST(amount_raw as decimal(38,0)) AS amount_raw,
  CASE WHEN erc.symbol = 'WMATIC' THEN 'MATIC' ELSE erc.symbol END AS currency_symbol,
  a.currency_contract,
  token_id,
  token_standard,
  a.contract_address AS project_contract_address,
  evt_type,
  CAST(NULL AS string) AS collection,
  CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END AS trade_type,
  CAST(number_of_items AS decimal(38,0)) AS number_of_items,
  CAST(NULL AS string) AS trade_category,
  buyer,
  seller,
  nft_contract_address,
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
  a.evt_tx_hash || '-' || a.evt_type  || '-' || a.evt_index ||  '-' || a.token_id || '-' || cast(a.number_of_items as string) AS unique_trade_id
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
LEFT JOIN {{ ref('tokens_erc20') }} erc ON erc.blockchain = 'polygon' AND erc.contract_address = a.currency_contract
LEFT JOIN price_list p ON p.contract_address = a.currency_contract AND p.minute = date_trunc('minute', a.evt_block_time)
LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.blockchain = 'polygon' AND agg.contract_address = t.`to`
