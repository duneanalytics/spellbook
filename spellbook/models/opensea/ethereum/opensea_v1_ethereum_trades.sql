 {{ config(schema = 'opensea_v1_ethereum', 
alias='trades') }}


WITH wyvern_call_data as (
SELECT 
  call_tx_hash,
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
  addrs [1] as buyer,
  addrs [8] AS seller,
  CASE WHEN contains('0xfb16a595', substring(calldataBuy,1,4)) THEN conv(substr(calldataBuy,203,64),16,10)::string
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
  trade_type,
  token_standard,
  project_contract_address,
  nft_contract_address,
  currency_contract,
  amount_original,
  buyer,
  seller,
  token_id,
  call_trace_address,
  currency_contract_original,
  fees,
  fees.to as fee_receive_address,
  fees.fee_currency_symbol
  FROM wyvern_call_data wc
  LEFT JOIN {{ ref('opensea_v1_ethereum_fees') }} fees ON fees.tx_hash = wc.call_tx_hash AND fees.trace_address = wc.call_trace_address),

erc_values_1155 as
(SELECT evt_tx_hash,
        id::string as token_id_erc,
        cardinality(collect_list(value)) as card_values,
        value as value_unique
        FROM {{ source('erc1155_ethereum','evt_transfersingle') }} erc1155
        WHERE erc1155.from NOT IN ('0x0000000000000000000000000000000000000000')
        GROUP BY evt_tx_hash,value,id),

-- Get ERC721 token ID and number of token IDs for every trade transaction 
erc_count_721 as
(SELECT evt_tx_hash,
        tokenId::string as token_id_erc,
        COUNT(tokenId) as count_erc
        FROM {{ source('erc721_ethereum','evt_transfer') }} erc721
        WHERE erc721.from NOT IN ('0x0000000000000000000000000000000000000000')
        GROUP BY evt_tx_hash,tokenId)
        
SELECT
  'ethereum' as blockchain,
  'opensea' as project,
  'v1' as version,
  tx.block_time,
  wa.token_id, 
  tokens_nft.name AS collection,
  wa.amount_original / power(10,erc20.decimals) * p.price AS amount_usd,
  CASE WHEN erc_values_1155.value_unique >= 1 THEN 'erc1155'
      WHEN erc_count_721.count_erc >= 1 THEN 'erc721'
      ELSE wa.token_standard END AS token_standard,
  CASE 
      WHEN agg.name is NULL AND erc_values_1155.value_unique = 1 OR erc_count_721.count_erc = 1 THEN 'Single Item Trade'
      WHEN agg.name is NULL AND erc_values_1155.value_unique > 1 OR erc_count_721.count_erc > 1 THEN 'Bundle Trade'
  ELSE wa.trade_type END AS trade_type,
  -- Count number of items traded for different trade types and erc standards
  CASE WHEN agg.name is NULL AND erc_values_1155.value_unique > 1 THEN erc_values_1155.value_unique
      WHEN agg.name is NULL AND erc_count_721.count_erc > 1 THEN erc_count_721.count_erc
      WHEN wa.trade_type = 'Single Item Trade' THEN cast(1 as bigint)
      WHEN wa.token_standard = 'erc1155' THEN erc_values_1155.value_unique
      WHEN wa.token_standard = 'erc721' THEN erc_count_721.count_erc
      ELSE (SELECT
              count(1)::bigint cnt
          FROM {{ source('erc721_ethereum','evt_transfer') }} erc721
          WHERE erc721.evt_tx_hash = wa.call_tx_hash
          AND erc721.from NOT IN ('0x0000000000000000000000000000000000000000')
        ) +    
        (SELECT
             count(1)::bigint cnt
          FROM {{ source('erc1155_ethereum','evt_transfersingle') }} erc1155
          WHERE erc1155.evt_tx_hash = wa.call_tx_hash
          AND erc1155.from NOT IN ('0x0000000000000000000000000000000000000000')
        ) END AS number_of_items,
  'Buy' AS trade_category,
  'Trade' AS evt_type,
  wa.seller AS seller,
  wa.buyer AS buyer,
  wa.amount_original / power(10,erc20.decimals) AS amount_original,
  wa.amount_original AS amount_raw,
  CASE WHEN wa.currency_contract_original = '0x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS currency_symbol,
  wa.currency_contract,
  wa.currency_contract_original AS currency_contract_original,
  wa.nft_contract_address AS nft_contract_address,
  wa.project_contract_address, 
  agg.name as aggregator_name,
  agg.contract_address as aggregator_address,
  wa.call_tx_hash AS tx_hash,
  tx.block_number,
  tx.from as tx_from,
  tx.to as tx_to,
  wa.fees AS fee_amount_raw,
  wa.fees / power(10,erc20.decimals) AS fee_amount,
  wa.fees / power(10,erc20.decimals) * p.price AS fee_amount_usd, 
  wa.fee_receive_address,
  wa.fee_currency_symbol,
  wa.call_tx_hash || '-' || wa.token_id || '-' || amount_original::string as unique_trade_id
FROM wyvern_all wa
LEFT JOIN {{ source('ethereum','transactions') }} tx ON wa.call_tx_hash = tx.hash
LEFT JOIN erc_values_1155 ON erc_values_1155.evt_tx_hash = wa.call_tx_hash AND wa.token_id = erc_values_1155.token_id_erc
LEFT JOIN erc_count_721 ON erc_count_721.evt_tx_hash = wa.call_tx_hash AND wa.token_id = erc_count_721.token_id_erc
LEFT JOIN {{ ref('tokens_ethereum_nft') }} tokens_nft ON tokens_nft.contract_address = wa.nft_contract_address 
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON agg.contract_address = tx.to
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', tx.block_time)
    AND p.contract_address = wa.currency_contract
    AND p.blockchain ='ethereum'
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20 ON erc20.contract_address = wa.currency_contract
 WHERE
        NOT EXISTS (SELECT * -- Exclude OpenSea mint transactions
            FROM {{ source('erc721_ethereum','evt_transfer') }} erc721
            WHERE wa.call_tx_hash = erc721.evt_tx_hash
            AND erc721.from = '0x0000000000000000000000000000000000000000')
  AND wa.call_tx_hash not in (
    SELECT
      *
    FROM
      {{ ref('opensea_v1_ethereum_excluded_txns') }}
  )
