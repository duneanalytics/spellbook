{{ config(schema='opensea_v3_ethereum'
         ,alias='trades')
}}

-- opensea.trades has the same columns as seaport.trades
-- only some specified zone_address are recognized as opensea's
-- project/version : opensea/v3
-- contract_address : 0x00000000006c3852cbef3e08e8df289169ede581 (Seaport v1.1)
-- materialize : view

select blockchain
      ,'opensea' as project
      ,'v3' as version
      ,block_date
      ,block_time
      ,seller
      ,buyer
      ,trade_type
      ,trade_category
      ,evt_type
      ,nft_contract_address
      ,collection
      ,token_id
      ,number_of_items
      ,token_standard
      ,amount_original
      ,amount_raw
      ,amount_usd
      ,currency_symbol
      ,currency_contract
      ,original_currency_contract
      ,currency_decimals
      ,project_contract_address
      ,platform_fee_receive_address
      ,platform_fee_amount_raw
      ,platform_fee_amount
      ,platform_fee_amount_usd
      ,royalty_fee_receive_address
      ,royalty_fee_amount_raw
      ,royalty_fee_amount
      ,royalty_fee_amount_usd
      ,royalty_fee_receive_address_1
      ,royalty_fee_receive_address_2
      ,royalty_fee_receive_address_3
      ,royalty_fee_receive_address_4
      ,royalty_fee_amount_raw_1
      ,royalty_fee_amount_raw_2
      ,royalty_fee_amount_raw_3
      ,royalty_fee_amount_raw_4
      ,aggregator_name
      ,aggregator_address
      ,block_number
      ,tx_hash
      ,evt_index
      ,tx_from
      ,tx_to
      ,right_hash
      ,zone_address
      ,estimated_price
      ,is_private
      ,'seaport-' || tx_hash || '-' || cast(evt_index as VARCHAR(100)) || '-' || CAST(nft_contract_address AS VARCHAR(100)) || '-' || cast(token_id as VARCHAR(100)) || '-' || cast(sub_idx as VARCHAR(100)) as unique_trade_id
  from {{ ref('seaport_ethereum_trades') }}
 where CAST(zone_address AS VARCHAR(100)) in ('0xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                       ,'0x9b814233894cd227f561b78cc65891aa55c62ad2'
                       ,'0x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                       ,'0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd'
                       )
   and version = 'v1'                       
