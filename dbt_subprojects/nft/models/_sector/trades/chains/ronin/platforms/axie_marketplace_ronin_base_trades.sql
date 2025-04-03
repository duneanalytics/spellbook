{{ config(
    schema = 'axie_marketplace_ronin',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}



with trade_details as (
  
  select  
  'v1' as version,
  call_block_time as block_time,
  date_trunc('day', call_block_time) as block_date,
  call_block_number as block_number,
  call_tx_hash as tx_hash,
  call_tx_from as tx_from,
  call_tx_to as tx_to,
  call_tx_index as evt_index,
  contract_address,
  from_hex(cast(json_extract(_order, '$.maker') as VARCHAR)) as seller,
  call_tx_from as buyer,
  cast(json_extract_scalar(_order, '$.kind') as UINT256) as kind,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order, '$.assets'), '$[0]'), '\\', ''), '$.erc') as UINT256) AS erc,
  from_hex(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.addr')) as nft_contract_address,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.id') as DOUBLE) as nft_token_id,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.quantity') as UINT256) as quantity,
  from_hex(cast(json_extract(_order, '$.paymentToken') as VARCHAR)) as currency_address,
  _settlePrice as price_raw,
  cast(json_extract(_order, '$.basePrice') as DOUBLE) as base_unit_price_raw,
  from_hex('0x245db945c485b68fdc429e4f7085a1761aa4d45d') as axie_treasury_address,
  cast(json_extract(_order, '$.marketFeePercentage') as DOUBLE)/10000 as axie_fee,
  cast(_settlePrice as DOUBLE) * cast(json_extract(_order, '$.marketFeePercentage') as DOUBLE)/10000  as axie_fee_amount_raw
  FROM
    {{ source('axie_marketplace_ronin','AppAxieOrderExchangeV1_call_settleOrder') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('call_block_time') }}
    AND call_success = true
    {% else %}
    WHERE call_success = true
    {% endif %}

UNION ALL

  select
  'v2' as version,
  call_block_time as block_time,
  date_trunc('day', call_block_time) as block_date,
  call_block_number as block_number,
  call_tx_hash as tx_hash,
  call_tx_from as tx_from,
  call_tx_to as tx_to,
  call_tx_index as evt_index,
  contract_address,
  from_hex(cast(json_extract(_order, '$.maker') as VARCHAR)) as seller,
  call_tx_from as buyer,
  cast(json_extract_scalar(_order, '$.kind') as UINT256) as kind,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order, '$.assets'), '$[0]'), '\\', ''), '$.erc') as UINT256) as erc,
  from_hex(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.addr')) as nft_contract_address,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.id') as DOUBLE) as nft_token_id,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.quantity') as DOUBLE) as quantity,
  case when element_at(_path,1)=0x0b7007c13325c48911f73a2dad5fa5dcbf808adc then from_hex(cast(json_extract(_order, '$.paymentToken') as VARCHAR))
  else element_at(_path,1) end as currency_address,
  _settlePrice as price_raw,
  cast(json_extract(_order, '$.basePrice') as DOUBLE) as base_unit_price_raw,
  from_hex('0x245db945c485b68fdc429e4f7085a1761aa4d45d') as axie_treasury_address,
  cast(json_extract(_order, '$.marketFeePercentage') as DOUBLE)/10000 as axie_fee,
  cast(_settlePrice as DOUBLE) * cast(json_extract(_order, '$.marketFeePercentage') as DOUBLE)/10000  as axie_fee_amount_raw

  FROM
    {{ source('axie_marketplace_ronin','AppAxieOrderExchangeV1_call_swapTokensAndSettleOrder') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('call_block_time') }}
    AND call_success = true
    {% else %}
    WHERE call_success = true
    {% endif %} 
  

  UNION ALL

  select
  'v3' as version,
  evt_block_time as block_time,
  evt_block_date as block_date,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_tx_from as tx_from,
  evt_tx_to as tx_to,
  evt_index,
  contract_address,
  coalesce(
            from_hex(json_extract_scalar(json_parse(element_at(receivedAllocs, cardinality(receivedAllocs))), '$.recipient')),
            from_hex(json_extract_scalar(json_parse(json_extract_scalar(m."order", '$.info')), '$.maker')),
            from_hex(json_extract_scalar(m."order",'$.maker'))
            ) as seller,
  coalesce(from_hex(json_extract_scalar(m."order", '$.recipient')),evt_tx_from) as buyer,
  coalesce(cast(json_extract_scalar(json_parse(json_extract_scalar(m."order", '$.info')), '$.kind') as UINT256),
            cast(json_extract_scalar(m."order", '$.kind') as UINT256)) as kind,
  coalesce(cast(json_extract_scalar(json_parse(replace(json_extract_scalar(json_extract(json_parse(json_extract_scalar(m."order", '$.info')), '$.assets'), '$[0]'), '\\', '')), '$.erc') as UINT256),
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(m."order", '$.assets'), '$[0]'), '\\', ''), '$.erc') as UINT256)) as erc,
  coalesce(from_hex(json_extract_scalar(replace(json_extract_scalar(json_parse(json_extract_scalar(m."order", '$.info')),'$.assets[0]'),'\\', ''),'$.addr')),
            from_hex(json_extract_scalar(replace(json_extract_scalar(json_extract(m."order", '$.assets'), '$[0]'), '\\', ''), '$.addr'))) AS nft_contract_address,
  
  coalesce(cast(json_extract_scalar(replace(json_extract_scalar(json_parse(json_extract_scalar(m."order", '$.info')),'$.assets[0]'),'\\', ''),'$.id') as double),
            cast(json_extract_scalar(replace(json_extract_scalar(json_extract(m."order", '$.assets'), '$[0]'), '\\', ''), '$.id') as double)) as nft_token_id,
  coalesce(cast(json_extract_scalar(replace(json_extract_scalar(json_parse(json_extract_scalar(m."order", '$.info')),'$.assets[0]'),'\\', ''),'$.quantity')as DOUBLE),
            cast(json_extract_scalar(replace(json_extract_scalar(json_extract(m."order", '$.assets'), '$[0]'), '\\', ''), '$.quantity') as UINT256)) as quantity,
  coalesce(from_hex(json_extract_scalar(json_parse(json_extract_scalar(m."order", '$.info')), '$.paymentToken')),
           from_hex(json_extract_scalar(m."order",'$.paymentToken'))) as currency_address,
  coalesce(cast(json_extract_scalar(m."order", '$.realPrice') as DOUBLE),cast(realPrice as DOUBLE)) as price_raw,
  coalesce(cast(json_extract_scalar(json_parse(json_extract_scalar(m."order", '$.info')), '$.baseUnitPrice') as DOUBLE),
                cast(json_extract_scalar(m."order",'$.basePrice') as DOUBLE)) as base_unit_price_raw,
  from_hex(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[2].recipient')) as axie_treasury_address,
  cast(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[2].ratio') AS DOUBLE) / 10000 as axie_fee,
  cast(json_extract_scalar(json_parse(cast(concat('[', array_join(receivedAllocs, ','), ']') as varchar)),'$[2].value') AS DOUBLE) as axie_fee_amount_raw
  FROM
    {{ source('axie_marketplace_ronin','MarketGateway_evt_OrderMatched') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

UNION ALL

  select
  'v1' as version,
  call_block_time as block_time,
  date_trunc('day', call_block_time) as block_date,
  call_block_number as block_number,
  call_tx_hash as tx_hash,
  call_tx_from as tx_from,
  call_tx_to as tx_to,
  call_tx_index as evt_index,
  contract_address,
  from_hex(cast(json_extract(_order, '$.maker') as VARCHAR)) as seller,
  call_tx_from as buyer,
  cast(json_extract_scalar(_order, '$.kind') as UINT256) as kind,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order, '$.assets'), '$[0]'), '\\', ''), '$.erc') as UINT256) as erc,
  from_hex(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.addr')) as nft_contract_address,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.id') as DOUBLE) as nft_token_id,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.quantity')
        as UINT256) as quantity,
  from_hex(cast(json_extract(_order, '$.paymentToken') as VARCHAR)) as currency_address,
  _settlePrice as price_raw,
  cast(json_extract(_order, '$.basePrice') as DOUBLE) as base_unit_price_raw,
  from_hex('0x245db945c485b68fdc429e4f7085a1761aa4d45d') as axie_treasury_address,
  cast(json_extract(_order, '$.marketFeePercentage') as DOUBLE)/10000 as axie_fee,
  cast(_settlePrice as DOUBLE) * cast(json_extract(_order, '$.marketFeePercentage') as DOUBLE)/10000  as axie_fee_amount_raw
  FROM
    {{ source('axie_marketplace_ronin','OrderExchangeLogic_call_settleOrder') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('call_block_time') }}
    AND call_success = true
    {% else %}
    WHERE call_success = true
    {% endif %} 
  
UNION ALL  
  
 select
  'v1' as version,
  call_block_time as block_time,
  date_trunc('day', call_block_time) as block_date,
  call_block_number as block_number,
  call_tx_hash as tx_hash,
  call_tx_from as tx_from,
  call_tx_to as tx_to,
  call_tx_index as evt_index,
  contract_address,
  from_hex(cast(json_extract(_order, '$.maker') as VARCHAR)) as seller,
  call_tx_from as buyer,
  cast(json_extract_scalar(_order, '$.kind') as UINT256) as kind,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order, '$.assets'), '$[0]'), '\\', ''), '$.erc') as UINT256) as erc,
  from_hex(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.addr')) as nft_contract_address,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.id') as DOUBLE) as nft_token_id,
  cast(json_extract_scalar(replace(json_extract_scalar(json_extract(_order,'$.assets'),'$[0]'), '\\', ''),'$.quantity')
        as UINT256) as quantity,
  element_at(_path,1) as currency_address,
  _settlePrice as price_raw,
  cast(json_extract(_order, '$.basePrice') as DOUBLE) as base_unit_price_raw,
  from_hex('0x245db945c485b68fdc429e4f7085a1761aa4d45d') as axie_treasury_address,
  cast(json_extract(_order, '$.marketFeePercentage') as DOUBLE)/10000 as axie_fee,
  cast(_settlePrice as DOUBLE) * cast(json_extract(_order, '$.marketFeePercentage') as DOUBLE)/10000  as axie_fee_amount_raw
  FROM
    {{ source('axie_marketplace_ronin','OrderExchangeLogic_call_swapTokensAndSettleOrder') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('call_block_time') }}
    AND call_success = true
    {% else %}
    WHERE call_success = true
    {% endif %} 


),

base_trades as (

   select 
  'ronin' as blockchain,
  'axie marketplace' as project,
  version as project_version,
  block_time,
  cast(date_trunc('day', block_time) as date) as block_date,
  cast(date_trunc('month', block_time) as date) as block_month,
  block_number,
  nft_contract_address,
  nft_token_id,
  quantity as nft_amount,
  seller,
  buyer,
  'buy' as trade_category,
  -- 'secondary' as trade_type, -- primary sales can take place too
  price_raw,
  currency_address as currency_contract,
  contract_address as project_contract_address,
  tx_hash,
  axie_treasury_address as platform_fee_address, -- since owner of nft collection is also the owner of the marketplace
  axie_fee_amount_raw as platform_fee_amount_raw,
  axie_treasury_address as royalty_fee_address, -- since owner of nft collection is also the owner of the marketplace
  axie_fee_amount_raw as royalty_fee_amount_raw,
  axie_fee_amount_raw as ronin_treasury_fee_amount_raw
  evt_index as sub_tx_trade_id
  FROM trade_details
  )

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'ronin') }}

  

