{{ config(
        alias ='transfers',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

with iv_raw as (
    select block_time
          ,block_slot
          ,id
          ,case when array_contains(log_messages, 'Program log: Instruction: Sell') then 'Offer Accepted'
                when array_contains(log_messages, 'Program log: Instruction: Buy') then 'Buy Now'
                else 'Other'
           end purchase_method
          ,case when array_contains(log_messages, 'Program log: Instruction: Sell') then instructions[6]
                when array_contains(log_messages, 'Program log: Instruction: Buy') then instructions[7]
                else instructions[7]
           end inst
      from {{ source('solana','transactions') }}
     where 1=1
       and success
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
       AND block_date > now() - interval 2 days
{% endif %} 
       and block_date > '2022-04-06'
       AND block_slot > 128251864
       and array_contains(account_keys, '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y') -- opensea auction house
       and array_contains(log_messages, 'Program log: Instruction: ExecuteSale')
)
,iv_raw2 as (
    select a.*
          ,case when inner_cnt > 3 then royalty_receive_address_cand end as royalty_receive_address
          ,case when inner_cnt = 4 then inner01 
                when inner_cnt = 5 then inner01+inner02
                when inner_cnt = 6 then inner01+inner02+inner03
                when inner_cnt = 7 then inner01+inner02+inner03+inner04 
                when inner_cnt = 8 then inner01+inner02+inner03+inner04+inner05
           end as royalty_amount_raw
          ,inner_cnt-3 as royalty_receiver_cnt
          ,case when inner_cnt = 4 then array(concat('{"receive_address":"',addr01,'","amount_raw":',inner01,'}'))
                when inner_cnt = 5 then array(concat('{"receive_address":"',addr01,'","amount_raw":',inner01,'}'),concat('{"receive_address":"',addr02,'","amount_raw":',inner02,'}'))
                when inner_cnt = 6 then array(concat('{"receive_address":"',addr01,'","amount_raw":',inner01,'}'),concat('{"receive_address":"',addr02,'","amount_raw":',inner02,'}'),concat('{"receive_address":"',addr03,'","amount_raw":',inner03,'}'))
                when inner_cnt = 7 then array(concat('{"receive_address":"',addr01,'","amount_raw":',inner01,'}'),concat('{"receive_address":"',addr02,'","amount_raw":',inner02,'}'),concat('{"receive_address":"',addr03,'","amount_raw":',inner03,'}'),concat('{"receive_address":"',addr04,'","amount_raw":',inner04,'}'))
                when inner_cnt = 8 then array(concat('{"receive_address":"',addr01,'","amount_raw":',inner01,'}'),concat('{"receive_address":"',addr02,'","amount_raw":',inner02,'}'),concat('{"receive_address":"',addr03,'","amount_raw":',inner03,'}'),concat('{"receive_address":"',addr04,'","amount_raw":',inner04,'}'),concat('{"receive_address":"',addr05,'","amount_raw":',inner05,'}'))
          end as royalty_list
      from (
            select a.*
                  ,conv(concat(substr(indata,15,2),substr(indata,13,2),substr(indata,11,2),substr(indata,9,2),substr(indata,7,2),substr(indata,5,2),substr(indata,3,2),substr(indata,1,2)),16,10)::numeric(20) as original_amount_raw
                  ,conv(concat(substr(fee_hex,15,2),substr(fee_hex,13,2),substr(fee_hex,11,2),substr(fee_hex,9,2),substr(fee_hex,7,2),substr(fee_hex,5,2),substr(fee_hex,3,2),substr(fee_hex,1,2)),16,10)::numeric(20) as fee_amount_raw
                  ,conv(concat(substr(hex01,15,2),substr(hex01,13,2),substr(hex01,11,2),substr(hex01,9,2),substr(hex01,7,2),substr(hex01,5,2),substr(hex01,3,2),substr(hex01,1,2)),16,10)::numeric(20) as inner01
                  ,conv(concat(substr(hex02,15,2),substr(hex02,13,2),substr(hex02,11,2),substr(hex02,9,2),substr(hex02,7,2),substr(hex02,5,2),substr(hex02,3,2),substr(hex02,1,2)),16,10)::numeric(20) as inner02
                  ,conv(concat(substr(hex03,15,2),substr(hex03,13,2),substr(hex03,11,2),substr(hex03,9,2),substr(hex03,7,2),substr(hex03,5,2),substr(hex03,3,2),substr(hex03,1,2)),16,10)::numeric(20) as inner03
                  ,conv(concat(substr(hex04,15,2),substr(hex04,13,2),substr(hex04,11,2),substr(hex04,9,2),substr(hex04,7,2),substr(hex04,5,2),substr(hex04,3,2),substr(hex04,1,2)),16,10)::numeric(20) as inner04
                  ,conv(concat(substr(hex05,15,2),substr(hex05,13,2),substr(hex05,11,2),substr(hex05,9,2),substr(hex05,7,2),substr(hex05,5,2),substr(hex05,3,2),substr(hex05,1,2)),16,10)::numeric(20) as inner05
              from (
                    select block_time
                          ,block_slot
                          ,id
                          ,purchase_method
                          ,inst.account_arguments[0] as buyer
                          ,inst.account_arguments[1] as seller
                          ,inst.account_arguments[3] as nft_contract_address
                          ,inst.account_arguments[5] as original_currency_address
                          ,inst.account_arguments[10] as exchange_contract_address
                          ,inst.inner_instructions[0].account_arguments[1] as royalty_receive_address_cand -- royalty can be divided into many addresses but we get first one only
                          ,inst.inner_instructions[(array_size(inst.inner_instructions)-3)].account_arguments[1] as fee_receive_address
                          ,substr(base58_decode(inst.data),23,16) as indata
                          ,substr(base58_decode(inst.inner_instructions[(array_size(inst.inner_instructions)-3)].data)::string,9,16) as fee_hex
                          ,array_size(inst.inner_instructions) as inner_cnt
                          ,inst.inner_instructions[0].account_arguments[1] as addr01
                          ,inst.inner_instructions[1].account_arguments[1] as addr02
                          ,inst.inner_instructions[2].account_arguments[1] as addr03
                          ,inst.inner_instructions[3].account_arguments[1] as addr04
                          ,inst.inner_instructions[4].account_arguments[1] as addr05
                          ,substr(base58_decode(inst.inner_instructions[0].data)::string,9,16) as hex01
                          ,substr(base58_decode(inst.inner_instructions[1].data)::string,9,16) as hex02
                          ,substr(base58_decode(inst.inner_instructions[2].data)::string,9,16) as hex03
                          ,substr(base58_decode(inst.inner_instructions[3].data)::string,9,16) as hex04
                          ,substr(base58_decode(inst.inner_instructions[4].data)::string,9,16) as hex05
                      from iv_raw
                    ) a
            ) a
)
,iv_nft_trades as (
    select  'solana' as blockchain
           ,'opensea' as project
           ,'v1' as version
           ,block_time
           ,nft_contract_address as token_id
           ,'' as collection
           ,original_amount_raw / 1e9 * p.price as amount_usd
           ,'metaplex' as token_standard
           ,'Single Item Trade' as trade_type
           ,'1' as number_of_items
           ,purchase_method as trade_category
           ,'Trade' as evt_type
           ,seller
           ,buyer
           ,original_amount_raw / 1e9 as amount_original
           ,original_amount_raw as amount_raw
           ,'SOL' as currency_symbol
           ,original_currency_address as currency_contract
           ,'' as nft_contract_address
           ,exchange_contract_address as project_contract_address
           ,'' as aggregator_name
           ,'' as aggregator_address
           ,id as tx_hash
           ,'' as tx_from
           ,'' as tx_to
           ,block_slot as block_number
           ,fee_receive_address as platform_fee_receive_address
           ,fee_amount_raw as platform_fee_amount_raw
           ,fee_amount_raw / 1e9 as platform_fee_amount
           ,fee_amount_raw / 1e9 * p.price as platform_fee_amount_usd
           ,fee_amount_raw / original_amount_raw * 100 as platform_fee_percentage
           ,royalty_receive_address as royalty_fee_receive_address
           ,royalty_amount_raw as royalty_fee_amount_raw
           ,royalty_amount_raw / 1e9 as royalty_fee_amount
           ,royalty_amount_raw / 1e9 * p.price as royalty_fee_amount_usd
           ,royalty_amount_raw / original_amount_raw * 100 as royalty_fee_percentage
           ,royalty_receiver_cnt
           ,royalty_list
           ,id as unique_trade_id
      from iv_raw2 a
           left join {{ source('prices', 'usd') }} p on p.minute = date_trunc('minute', block_time)
                                  and p.symbol = 'SOL'
)
select *
  from iv_nft_trades