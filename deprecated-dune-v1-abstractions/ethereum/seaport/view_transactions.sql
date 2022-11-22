create schema if not exists seaport;

drop view if exists seaport.view_transactions cascade;

create or replace view seaport.view_transactions 
as
select block_time
      ,nft_project_name
      ,nft_token_id
      ,erc_standard
      ,platform
      ,'3' as platform_version
      ,trade_type
      ,nft_item_count as number_of_items
      ,order_type
      ,'Trade' as evt_type
      ,usd_amount
      ,seller
      ,buyer
      ,original_amount
      ,original_amount_raw
      ,original_currency
      ,original_currency_contract
      ,currency_contract
      ,nft_contract_address
      ,exchange_contract_address
      ,tx_hash
      ,block_number
      ,tx_from
      ,tx_to
      ,0 as evt_index
      ,fee_receive_address
      ,case when fee_amount > 0 then original_currency end as fee_currency
      ,fee_amount_raw
      ,fee_amount
      ,fee_usd_amount
      ,royalty_receive_address
      ,case when royalty_amount > 0 then original_currency end as royalty_currency
      ,royalty_amount_raw
      ,royalty_amount
      ,royalty_usd_amount
      ,zone_address
  from seaport.transactions
;