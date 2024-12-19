{{ config(
    schema = 'mavis_ronin',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}


with trade_details as (

select 
  evt_block_time as block_time,
  evt_block_date as block_date,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_tx_from  as tx_from,
  evt_tx_to as tx_to,
  evt_index,
  contract_address,
  COALESCE(FROM_HEX(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[5].recipient')),FROM_HEX(json_extract_scalar(json_parse(json_extract_scalar("order", '$.info')), '$.maker'))) as seller,
  FROM_HEX(json_extract_scalar("order", '$.recipient')) as buyer,
  json_extract_scalar(json_parse(json_extract_scalar("order", '$.info')), '$.kind') as kind,
  cast(json_extract_scalar(replace(json_extract_scalar(json_parse(json_extract_scalar("order", '$.info')),'$.assets[0]'),'\\', ''),'$.erc') as double) as erc,
  FROM_HEX(json_extract_scalar(replace(json_extract_scalar(json_parse(json_extract_scalar("order", '$.info')),'$.assets[0]'),'\\', ''),'$.addr')) as nft_contract_address,
  cast(json_extract_scalar(replace(json_extract_scalar(json_parse(json_extract_scalar("order", '$.info')),'$.assets[0]'),'\\', ''),'$.id') as double) as nft_token_id,
  cast(json_extract_scalar(replace(json_extract_scalar(json_parse(json_extract_scalar("order", '$.info')),'$.assets[0]'),'\\', ''),'$.quantity') as double) as quantity,
  FROM_HEX(json_extract_scalar(json_parse(json_extract_scalar("order", '$.info')), '$.paymentToken')) as currency_address,
  CAST(json_extract_scalar("order", '$.realPrice') AS DOUBLE) as price_raw,
  CAST(json_extract_scalar(json_parse(json_extract_scalar("order", '$.info')), '$.baseUnitPrice') AS DOUBLE) as base_unit_price_raw,
  FROM_HEX(json_extract_scalar("order", '$.refunder')) as refunder,
  FROM_HEX(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[1].recipient')) as platform_address,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[1].ratio') AS DOUBLE) as platform_fee_amount_raw,
  FROM_HEX(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[2].recipient')) as axie_treasury_address,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[2].ratio') AS DOUBLE) as axie_fee_raw,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[2].value') AS DOUBLE) as axie_fee_amount_raw,
  FROM_HEX(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[3].recipient')) as ronin_treasury_address,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[3].ratio') AS DOUBLE) as ronin_fee,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[3].value') AS DOUBLE) as ronin_treasury_fee_amount_raw,
  FROM_HEX(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[4].recipient')) as creator_royalty_address,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[4].ratio') AS DOUBLE) as creator_royalty_fee,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[4].value') AS DOUBLE) as creator_royalty_fee_amount_raw,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[5].ratio')AS DOUBLE) AS seller_percentage_fee,
  CAST(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[5].value')AS DOUBLE) AS seller_amount_raw
  FROM
    {{ source('mavis_marketplace_ronin','MavisMarketPlace_evt_OrderMatched') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
  ),

base_trades as (

  select 
  'ronin' as blockchain,
  'mavis market' as project,
  'v1' as project_version,
  block_time,
  cast(date_trunc('day', block_time) as date) as block_date,
  cast(date_trunc('month', block_time) as date) as block_month,
  block_number,
  nft_contract_address,
  nft_token_id,
  quantity as nft_amount,
  seller,
  buyer,
  '' as trade_category,
  '' as trade_type,
  price_raw,
  currency_address as currency_contract,
  contract_address as project_contract_address,
  tx_hash,
  platform_address as platform_fee_address,
  platform_fee_amount_raw,
  creator_royalty_address as royalty_fee_address,
  creator_royalty_fee_amount_raw as royalty_fee_amount_raw,
  -- axie_fee_amount_raw,
  -- ronin_treasury_fee_amount_raw,
  evt_index as sub_tx_trade_id
  FROM trade_details
  )
  
-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'ronin') }}


  
