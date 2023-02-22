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

WITH nft_order AS (
    SELECT contract_address,
          direction,
          evt_block_time,
          evt_block_number,
          evt_tx_hash,
          evt_index,
          CASE when direction = 0 THEN taker ELSE maker END AS buyer,
          CASE when direction = 0 THEN maker ELSE taker END AS seller,
          matcher,
          erc721Token AS nft_contract_address,
          '' as collection,
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
    WHERE 1 = 1 
        AND evt_block_time >= '{{nft_start_date}}'

    UNION ALL

    SELECT contract_address,
          direction,
          evt_block_time,
          evt_block_number,
          evt_tx_hash,
          evt_index,
          CASE when direction = 0 THEN taker ELSE maker END AS buyer,
          CASE when direction = 0 THEN maker ELSE taker END AS seller,
          matcher,
          erc1155Token AS nft_contract_address,
          '' as collection,
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
    WHERE 1 = 1 
        AND evt_block_time >= '{{nft_start_date}}'
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
) 

SELECT
  'polygon' as blockchain,
  'magiceden' as project,
  'v1' as version,
  o.evt_tx_hash as tx_hash,
  date_trunc('day', o.evt_block_time) as block_date,
  o.evt_block_time as block_time,
  o.evt_block_number as block_number,
  amount_raw / power(10, p.decimals) * p.price AS amount_usd,
  amount_raw / power(10, p.decimals) AS amount_original,
  amount_raw,
  CASE WHEN p.symbol = 'WMATIC' THEN 'MATIC' ELSE p.symbol END as currency_symbol,
  p.contract_address as currency_contract,
  token_id,
  token_standard,
  o.contract_address as project_contract_address,
  'Trade' as evt_type,
  NULL::string as collection,
  CASE WHEN number_of_items = 1 THEN 'Single Item Trade' ELSE 'Bundle Trade' END as trade_type,
  number_of_items,
  NULL::string as trade_category,
  buyer,
  seller,
  nft_contract_address,
  NULL::string as aggregator_name,
  NULL::string as aggregator_address,
  t.`from` as tx_from,
  t.`to` as tx_to,
  2 * amount_raw / 100 as platform_fee_amount_raw,
  2 * amount_raw / power(10, p.decimals) / 100 as platform_fee_amount,
  2 * amount_raw / power(10, p.decimals) * p.price / 100 as platform_fee_amount_usd,
  CAST(2 AS DOUBLE) as platform_fee_percentage,
  0 royalty_fee_amount,
  0 as royalty_fee_amount_usd,
  0 as royalty_fee_percentage,
  NULL::double as royalty_fee_receive_address,
  NULL::string as royalty_fee_currency_symbol,
  evt_tx_hash || '-' || evt_index || '-' || token_id  as unique_trade_id
FROM nft_order o
INNER JOIN {{ source('polygon','transactions') }} t ON o.evt_block_number = t.block_number
     AND o.evt_tx_hash = t.hash
     AND t.block_time >= '{{nft_start_date}}'
LEFT JOIN price_list p ON p.contract_address = o.currency_contract AND p.minute = date_trunc('minute', o.evt_block_time)
WHERE 1 = 1
     AND o.evt_block_time >= '{{nft_start_date}}'
