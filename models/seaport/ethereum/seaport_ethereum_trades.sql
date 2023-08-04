{{ config(
    alias = alias('trades'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "seaport",
                            \'["sohwak", "hildobby" ,"soispoke" ,"jeff-dude" ,"hosuke" ,"0xRob"]\') }}'
    )
}}

select *
  from (
        select blockchain
              ,project
              ,version
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
              ,sub_idx
              ,sub_type
          from {{ ref('seaport_v1_ethereum_trades') }}
        union all
        select blockchain
              ,project
              ,version
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
              ,sub_idx
              ,sub_type
          from {{ ref('seaport_v2_ethereum_trades') }}                  
        )
