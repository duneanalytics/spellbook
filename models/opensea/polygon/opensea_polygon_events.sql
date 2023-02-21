{{ config(
    schema = 'opensea_polygon',
    alias = 'events',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['block_date']
    )
}}

{% set START_DATE='2021-06-28' %}
{% set END_DATE='2022-10-07' %}

WITH wyvern_call_data as (
   select 
      call_tx_hash as tx_hash,
      call_block_time as block_time,
      call_block_number as block_number,
      a.contract_address as project_contract_address,
      a.leftOrder:makerAddress buyer,
      a.rightOrder:makerAddress as seller,
      'Sell' as trade_category, --TODO
      'Fixed price'as sale_type,
      paymentTokenAddress as currency_contract,
      feeData[0]:recipient as fee_recipient,
      'Trade' as evt_type,
      0.025 as platform_fee,
      case when length(feeData[1]:recipient) > 0 then 0.025 else 0 end as royalty_fee,
      'Single Item Trade' as trade_type,
      '0x' || right(substring(a.rightOrder:makerAssetData, 11, 64), 40) as nft_contract_address,
      case when length(a.rightOrder:makerAssetData) = 650 then bytea2numeric_v3(substr(a.rightOrder:makerAssetData,332,64))::string
            else bytea2numeric_v3(substr(a.rightOrder:makerAssetData,76,64))::string
       end as token_id,
      case when length(a.rightOrder:makerAssetData) = 650 then 'erc1155'
            else 'erc721' -- 138
       end as token_standard, 
      least((output_matchedFillResults:left:takerFeePaid)::numeric, (output_matchedFillResults:right:makerFeePaid)::numeric) as number_of_items,
      least((output_matchedFillResults:left:makerFeePaid)::decimal(38,0), (output_matchedFillResults:right:takerFeePaid)::decimal(38,0)) as amount_raw,
      (feeData[0]:paymentTokenAmount)::decimal(38,0) as fee_amount_raw,
      feeData[1]:recipient as royalty_receive_address,
      (feeData[1]:paymentTokenAmount)::decimal(38,0) as royalty_amount_raw,
      a.contract_address as exchange_contract_address
   from {{ source('opensea_polygon_v2_polygon','ZeroExFeeWrapper_call_matchOrders') }} a
   where 1=1
     and call_success
     and a.call_block_time >= '{{START_DATE}}'
     and a.call_block_time <= '{{END_DATE}}'
)

SELECT
  'polygon' as blockchain,
  'opensea' as project,
  'v2' as version,
  project_contract_address,
  TRY_CAST(date_trunc('DAY', t.block_time) AS date) AS block_date,
  t.block_time,
  t.block_number,
  t.tx_hash,
  t.nft_contract_address,
  t.token_standard,
  '' AS collection, -- Currently there is no data for Polygon
  t.token_id,
  t.amount_raw,
  t.amount_raw / power(10,erc20.decimals) as amount_original,
  t.amount_raw / power(10,erc20.decimals) * p.price AS amount_usd,
  t.trade_category,
  t.trade_type,
  t.number_of_items,
  t.seller,
  t.buyer,
  t.evt_type,
  erc20.symbol AS currency_symbol,
  t.currency_contract,
--  agg.name as aggregator_name,
--  agg.contract_address as aggregator_address,
--  tx.from as tx_from,
--  tx.to as tx_to,
  CAST(NULL AS VARCHAR(5)) as aggregator_name,
  CAST(NULL AS VARCHAR(5)) as aggregator_address,
  CAST(NULL AS VARCHAR(5)) as tx_from,
  CAST(NULL AS VARCHAR(5)) as tx_to,
  CAST(round((100 * platform_fee),4) AS DOUBLE) AS platform_fee_percentage,
  platform_fee * t.amount_raw AS platform_fee_amount_raw,
  platform_fee * t.amount_raw / power(10,erc20.decimals) AS platform_fee_amount,
  platform_fee * t.amount_raw / power(10,erc20.decimals) * p.price AS platform_fee_amount_usd,
  CAST(round((100 * royalty_fee),4) AS DOUBLE) as royalty_fee_percentage,
  royalty_fee * t.amount_raw AS royalty_fee_amount_raw,
  royalty_fee * t.amount_raw / power(10,erc20.decimals) AS royalty_fee_amount,
  royalty_fee * t.amount_raw / power(10,erc20.decimals) * p.price AS royalty_fee_amount_usd,
  t.fee_recipient as royalty_fee_receive_address,
  erc20.symbol AS royalty_fee_currency_symbol,
  'wyvern-opensea' || '-' || t.tx_hash || '-' || token_id as unique_trade_id
FROM wyvern_call_data t
-- When join transactions table, compiled query timed out. So ignore them
-- INNER JOIN {{ source('polygon','transactions') }} tx ON t.block_number = tx.block_number AND t.tx_hash = tx.hash
--     AND tx.block_time >= '{{START_DATE}}' AND tx.block_time <= '{{END_DATE}}'
-- LEFT JOIN {{ ref('nft_aggregators') }} agg ON agg.contract_address = tx.to AND agg.blockchain = 'polygon'
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', t.block_time)
    AND p.contract_address = t.currency_contract
    AND p.blockchain ='polygon'
    AND minute >= '{{START_DATE}}' AND minute <= '{{END_DATE}}'
LEFT JOIN {{ ref('tokens_erc20') }} erc20 ON erc20.contract_address = t.currency_contract and erc20.blockchain = 'polygon'
;