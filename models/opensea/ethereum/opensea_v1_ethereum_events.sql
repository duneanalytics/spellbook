{{ config(
    schema = 'opensea_v1_ethereum',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

WITH wyvern_call_data as (
SELECT
  call_tx_hash,
  call_block_time,
  CASE WHEN contains('0x68f0bcaa', substring(calldataBuy,1,4)) THEN 'Bundle Trade'
        ELSE 'Single Item Trade'
  END AS trade_type,
  CASE WHEN contains('0xfb16a595', substring(calldataBuy,1,4)) THEN 'erc721'
        WHEN contains('0x23b872dd', substring(calldataBuy,1,4)) THEN 'erc721'
        WHEN contains('0x96809f90', substring(calldataBuy,1,4)) THEN 'erc1155'
        WHEN contains('0xf242432a', substring(calldataBuy,1,4)) THEN 'erc1155'
  END AS token_standard,
  addrs [0] as project_contract_address,
  CASE WHEN contains('0xfb16a595', substring(calldataBuy,1,4)) THEN '0x'||substr(calldataBuy,163,40)
        WHEN contains('0x96809f90', substring(calldataBuy,1,4)) THEN '0x'||substr(calldataBuy,163,40)
        WHEN contains('0x23b872dd', substring(calldataBuy,1,4)) THEN addrs [4]
        WHEN contains('0xf242432a', substring(calldataBuy,1,4)) THEN addrs [4]
        END AS nft_contract_address,
  CASE -- Replace `ETH` with `WETH` for ERC20 lookup later
      WHEN addrs [6] = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
      ELSE addrs [6]
  END AS currency_contract,
  uints [4] as amount_original,
  addrs[4] as shared_storefront_address,
  addrs [1] as buyer,
  addrs [8] AS seller,
  -- Temporary fix for token ID until we implement a UDF equivalent for bytea2numeric that works for numbers higher than 64 bits
  CASE WHEN contains('0xfb16a595', substring(calldataBuy,1,4)) AND conv(substr(calldataBuy,203,64),16,10)::string = '18446744073709551615'
  THEN 'Token ID is larger than 64 bits and can not be displayed'
  WHEN contains('0x96809f90', substring(calldataBuy,1,4)) AND conv(substr(calldataBuy,203,64),16,10)::string = '18446744073709551615'
  THEN 'Token ID is larger than 64 bits and can not be displayed'
  WHEN contains('0x23b872dd', substring(calldataBuy,1,4)) AND conv(substr(calldataBuy,139,64),16,10)::string = '18446744073709551615'
  THEN 'Token ID is larger than 64 bits and can not be displayed'
  WHEN contains('0xf242432a', substring(calldataBuy,1,4)) AND conv(substr(calldataBuy,139,64),16,10)::string = '18446744073709551615'
  THEN 'Token ID is larger than 64 bits and can not be displayed'
  WHEN contains('0xfb16a595', substring(calldataBuy,1,4)) THEN conv(substr(calldataBuy,203,64),16,10)::string
  WHEN contains('0x96809f90', substring(calldataBuy,1,4)) THEN conv(substr(calldataBuy,203,64),16,10)::string
  WHEN contains('0x23b872dd', substring(calldataBuy,1,4)) THEN conv(substr(calldataBuy,139,64),16,10)::string
  WHEN contains('0xf242432a', substring(calldataBuy,1,4)) THEN conv(substr(calldataBuy,139,64),16,10)::string
  END AS token_id,
  CASE WHEN size(call_trace_address) = 0 then array(3::bigint) -- for bundle join
  ELSE call_trace_address
  END as call_trace_address,
  addrs [6] AS currency_contract_original
FROM
  {{ source('opensea_ethereum','wyvernexchange_call_atomicmatch_') }} wc
WHERE
(addrs[3] = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
        OR addrs[10] = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073')
AND call_success = true
),

wyvern_all as
(
SELECT
  call_tx_hash,
  call_block_time,
  trade_type,
  token_standard,
  project_contract_address,
  nft_contract_address,
  currency_contract,
  amount_original,
  shared_storefront_address,
  buyer,
  seller,
  token_id,
  call_trace_address,
  currency_contract_original,
  fees,
  fees.to as fee_receive_address,
  fees.fee_currency_symbol,
  call_trace_address
  FROM wyvern_call_data wc
  LEFT JOIN {{ ref('opensea_v1_ethereum_fees') }} fees ON fees.tx_hash = wc.call_tx_hash AND fees.trace_address = wc.call_trace_address),

erc_transfers as
(SELECT evt_tx_hash,
        CASE WHEN length(id::string) > 64 THEN 'Token ID is larger than 64 bits and can not be displayed' ELSE id::string END as token_id_erc,
        cardinality(collect_list(value)) as count_erc,
        value as value_unique,
        CASE WHEN erc1155.from = '0x0000000000000000000000000000000000000000' THEN 'Mint'
        WHEN erc1155.to = '0x0000000000000000000000000000000000000000'
        OR erc1155.to = '0x000000000000000000000000000000000000dead' THEN 'Burn'
        ELSE 'Trade' END AS evt_type,
        evt_index
        FROM {{ source('erc1155_ethereum','evt_transfersingle') }} erc1155
        GROUP BY evt_tx_hash,value,id,evt_index, erc1155.from, erc1155.to
            UNION ALL
SELECT evt_tx_hash,
        CASE WHEN length(tokenId::string) > 64 THEN 'Token ID is larger than 64 bits and can not be displayed' ELSE tokenId::string END as token_id_erc,
        COUNT(tokenId) as count_erc,
        NULL as value_unique,
        CASE WHEN erc721.from = '0x0000000000000000000000000000000000000000' THEN 'Mint'
        WHEN erc721.to = '0x0000000000000000000000000000000000000000'
        OR erc721.to = '0x000000000000000000000000000000000000dead' THEN 'Burn'
        ELSE 'Trade' END AS evt_type,
        evt_index
        FROM {{ source('erc721_ethereum','evt_transfer') }} erc721
        GROUP BY evt_tx_hash,tokenId,evt_index, erc721.from, erc721.to)

SELECT DISTINCT
  'ethereum' as blockchain,
  'opensea' as project,
  'v1' as version,
  TRY_CAST(date_trunc('DAY', wa.call_block_time) AS date) AS block_date,
  tx.block_time,
  coalesce(token_id_erc, wa.token_id) as token_id,
  tokens_nft.name AS collection,
  wa.amount_original / power(10,erc20.decimals) * p.price AS amount_usd,
  CASE WHEN erc_transfers.value_unique >= 1 THEN 'erc1155'
      WHEN erc_transfers.value_unique is null THEN 'erc721'
      ELSE wa.token_standard END AS token_standard,
  CASE
      WHEN agg.name is NULL AND erc_transfers.value_unique = 1 OR erc_transfers.count_erc = 1 THEN 'Single Item Trade'
      WHEN agg.name is NULL AND erc_transfers.value_unique > 1 OR erc_transfers.count_erc > 1 THEN 'Bundle Trade'
  ELSE wa.trade_type END AS trade_type,
  -- Count number of items traded for different trade types and erc standards
  CASE WHEN agg.name is NULL AND erc_transfers.value_unique > 1 THEN erc_transfers.value_unique
      WHEN agg.name is NULL AND erc_transfers.value_unique is NULL AND erc_transfers.count_erc > 1 THEN erc_transfers.count_erc
      WHEN wa.trade_type = 'Single Item Trade' THEN cast(1 as bigint)
      WHEN wa.token_standard = 'erc1155' THEN erc_transfers.value_unique
      WHEN wa.token_standard = 'erc721' THEN erc_transfers.count_erc
      ELSE (SELECT
              count(1)::bigint cnt
          FROM {{ source('erc721_ethereum','evt_transfer') }} erc721
          WHERE erc721.evt_tx_hash = wa.call_tx_hash
        ) +
        (SELECT
             count(1)::bigint cnt
          FROM {{ source('erc1155_ethereum','evt_transfersingle') }} erc1155
          WHERE erc1155.evt_tx_hash = wa.call_tx_hash
        ) END AS number_of_items,
  'Buy' AS trade_category,
  wa.seller AS seller,
  wa.buyer AS buyer,
  CASE WHEN shared_storefront_address = '0x495f947276749ce646f68ac8c248420045cb7b5e' THEN 'Mint'
  WHEN evt_type is not NULL THEN evt_type ELSE 'Trade' END as evt_type,
  wa.amount_original / power(10,erc20.decimals) AS amount_original,
  wa.amount_original AS amount_raw,
  CASE WHEN wa.currency_contract_original = '0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS currency_symbol,
  wa.currency_contract,
  wa.nft_contract_address AS nft_contract_address,
  wa.project_contract_address,
  agg.name as aggregator_name,
  agg.contract_address as aggregator_address,
  tx.block_number,
  wa.call_tx_hash AS tx_hash,
  tx.from as tx_from,
  tx.to as tx_to,
  ROUND((2.5*(wa.amount_original)/100),7) AS platform_fee_amount_raw,
  ROUND((2.5*(wa.amount_original / power(10,erc20.decimals))/100),7) AS platform_fee_amount,
  ROUND((2.5*(wa.amount_original / power(10,erc20.decimals) * p.price)/100),7) AS platform_fee_amount_usd,
  '2.5' AS platform_fee_percentage,
  wa.fees AS royalty_fee_amount_raw,
  wa.fees / power(10,erc20.decimals) AS royalty_fee_amount,
  wa.fees / power(10,erc20.decimals) * p.price AS royalty_fee_amount_usd,
  (wa.fees / wa.amount_original * 100)::string  AS royalty_fee_percentage,
  wa.fee_receive_address as royalty_fee_receive_address,
  wa.fee_currency_symbol as royalty_fee_currency_symbol,
  'opensea' || '-' || wa.call_tx_hash || '-' || coalesce(wa.token_id, token_id_erc, '') || '-' ||  wa.seller || '-' || coalesce(evt_index::string, '') || '-' || coalesce(wa.call_trace_address::string,'') as unique_trade_id
FROM wyvern_all wa
LEFT JOIN {{ source('ethereum','transactions') }} tx ON wa.call_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN erc_transfers ON erc_transfers.evt_tx_hash = wa.call_tx_hash AND (wa.token_id = erc_transfers.token_id_erc
OR wa.token_id = null)
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tokens_nft ON tokens_nft.contract_address = wa.nft_contract_address
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address = tx.to
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', tx.block_time)
    AND p.contract_address = wa.currency_contract
    AND p.blockchain ='ethereum'
    {% if is_incremental() %}
    AND p.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20 ON erc20.contract_address = wa.currency_contract
  WHERE wa.call_tx_hash not in (
    SELECT
      *
    FROM
      {{ ref('opensea_v1_ethereum_excluded_txns') }})
