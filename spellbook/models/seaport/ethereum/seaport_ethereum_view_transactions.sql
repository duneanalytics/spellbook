{{
  config(
        alias='view_transactions'
  )
}}

with iv_availadv as (
    select 'avail' as order_type
          ,'avail' as sub_type
          ,exec_idx as sub_idx
          ,exec:offerer as sender 
          ,exec:item:recipient as receiver
          ,advancedOrders[0]:parameters:zone as zone 
          ,exec:item:token as token_contract_address
          ,exec:item:amount as original_amount
          ,exec:item:itemType as item_type
          ,exec:item:identifier as token_id
          ,contract_address as exchange_contract_address
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,exec_idx as evt_index 
      from (select *
                  ,posexplode(output_executions) as (exec_idx, exec)
              from {{ source('seaport_ethereum','Seaport_call_fulfillAvailableAdvancedOrders') }} a
             where call_success
            )
)
,iv_transfer_level_pre as ( 
    select 'normal' as main_type
          ,'offer' as sub_type
          ,rn + 1 as sub_idx
          ,offerer as sender
          ,recipient as receiver
          ,zone
          ,each_offer:token as token_contract_address 
          ,each_offer:amount::bigint as original_amount
          ,each_offer:itemType as item_type
          ,each_offer:identifier as token_id
          ,contract_address as exchange_contract_address
          ,evt_tx_hash as tx_hash
          ,evt_block_time as block_time
          ,evt_block_number as block_number
          ,evt_index
    from (select *
                ,posexplode(offer) as (rn, each_offer)
            from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }}  a
           where 1=1
            and recipient != '0x0000000000000000000000000000000000000000'
         )
    union all
    select 'normal' as main_type
          ,'consideration' as sub_type
          ,rn + 1 as sub_idx
          ,recipient as sender
          ,each_consideration:recipient as receiver
          ,zone
          ,each_consideration:token as token_contract_address
          ,each_consideration:amount::bigint as original_amount
          ,each_consideration:itemType as item_type
          ,each_consideration:identifier as token_id
          ,contract_address as exchange_contract_address
          ,evt_tx_hash as tx_hash
          ,evt_block_time as block_time
          ,evt_block_number as block_number
          ,evt_index
      from (select *
                  ,posexplode(consideration) as (rn, each_consideration)
              from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }} a
             where 1=1
              and recipient != '0x0000000000000000000000000000000000000000'
          )
    union all
    select 'advanced' as main_type
          ,'mix' as sub_type
          ,a.rn + 1 as sub_idx
          ,e.offerer as sender
          ,a.each_consideration:recipient as receiver
          ,a.zone
          ,a.each_consideration:token as token_contract_address
          ,a.each_consideration:amount::bigint as original_amount
          ,a.each_consideration:itemType as item_type
          ,a.each_consideration:identifier as token_id
          ,a.contract_address as exchange_contract_address
          ,a.evt_tx_hash as tx_hash
          ,a.evt_block_time as block_time
          ,a.evt_block_number as block_number
          ,a.evt_index
     from (select *
                  ,posexplode(consideration) as (rn, each_consideration)
              from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }} a
             where 1=1
               and recipient = '0x0000000000000000000000000000000000000000'
           ) a
          inner join  (select *
                              ,posexplode(offer) as (rn, each_offer)
                          from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }} a
                         where 1=1
                           and recipient = '0x0000000000000000000000000000000000000000'
                       ) e on a.recipient = e.recipient 
                          and a.evt_tx_hash = e.evt_tx_hash
                          and a.each_consideration:token = e.each_offer:token
                          and a.each_consideration:itemType = e.each_offer:itemType
                          and a.each_consideration:identifier = e.each_offer:identifier

)
,iv_transfer_level as (
    select a.*
      from iv_transfer_level_pre a
           left join iv_availadv b on b.tx_hash = a.tx_hash
                                   and b.item_type in ('2','3')
                                   and b.token_contract_address = a.token_contract_address
                                   and b.token_id = a.token_id
                                   and b.sender = a.sender
                                   and b.receiver = a.receiver
           left join {{ source('seaport_ethereum','Seaport_call_fulfillAvailableAdvancedOrders') }} c on c.call_tx_hash = a.tx_hash
     where 1=1 
       and not (a.item_type in ('2','3') and b.tx_hash is null and c.call_tx_hash is not null)
)
,iv_txn_level as (
    select tx_hash
          ,block_time
          ,block_number
          ,0 as evt_index
          ,category
          ,exchange_contract_address
          ,zone
          ,max(case when item_type in ('2','3') then sender end) as seller
          ,max(case when item_type in ('2','3') then receiver end) as buyer
          ,sum(case when category = 'auction' and sub_idx in (1,2) then original_amount
                    when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then original_amount
                    when category = 'click buy now' and sub_type = 'consideration' then original_amount
              end) as original_amount
          ,max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address
                    when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address
                    when category = 'click buy now' and sub_type = 'consideration' then token_contract_address
              end) as original_currency_contract
          ,case when max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address
                            when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address
                            when category = 'click buy now' and sub_type = 'consideration' then token_contract_address
                      end) = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address
                            when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address
                            when category = 'click buy now' and sub_type = 'consideration' then token_contract_address
                      end)
            end as currency_contract
          ,max(case when category = 'auction' and sub_idx = 2 then receiver
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then receiver
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then receiver
              end) as fee_receive_address
          ,sum(case when category = 'auction' and sub_idx = 2 then original_amount
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then original_amount
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then original_amount
              end) as fee_amount
          ,max(case when category = 'auction' and sub_idx = 2 then token_contract_address
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address
              end) as fee_currency_contract
          ,case when max(case when category = 'auction' and sub_idx = 2 then token_contract_address
                            when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address
                            when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address
                      end) = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else max(case when category = 'auction' and sub_idx = 2 then token_contract_address
                            when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address
                            when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address
                      end)
          end as currency_contract2
          ,max(case when nft_transfer_count = 1 and item_type in ('2','3') then token_contract_address 
              end) as nft_contract_address
          ,max(case when nft_transfer_count = 1 and item_type in ('2','3') then token_id
              end) as nft_token_id
          ,count(case when item_type = '2' then 1 end) as erc721_transfer_count
          ,count(case when item_type = '3' then 1 end) as erc1155_transfer_count
          ,count(case when item_type in ('2','3') then 1 end) as nft_transfer_count
          ,coalesce(sum(case when item_type = '2' then original_amount end),0) as erc721_item_count
          ,coalesce(sum(case when item_type = '3' then original_amount end),0) as erc1155_item_count
          ,coalesce(sum(case when item_type in ('2','3') then original_amount end),0) as nft_item_count
      from (
            select a.*
                  ,count(case when item_type in ('2','3') then 1 end) over (partition by tx_hash) as nft_transfer_count
                  ,case when main_type = 'advanced' then 'auction'
                        when max(case when item_type in ('0','1') then item_type end) over (partition by tx_hash) = '0' then 'click buy now' 
                        else 'offer accepted' 
                  end as category
                  ,case when (item_type, sub_idx) in (('2',1),('3',1)) then True
                        when main_type = 'advanced' and sub_idx = 3 then True 
                  end as first_item 
              from iv_transfer_level a
            ) a
     group by 1,2,3,4,5,6,7
)
,iv_nft_trades as ( 
    select a.block_time
          ,n.name as nft_project_name
          ,nft_token_id
          ,case when erc721_transfer_count > 0 and erc1155_transfer_count = 0 then 'erc721'
                when erc721_transfer_count = 0 and erc1155_transfer_count > 0 then 'erc1155'
                when erc721_transfer_count > 0 and erc1155_transfer_count > 0 then 'mixed'
           end as erc_standard
          ,case when a.zone in ('0xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                               ,'0x9b814233894cd227f561b78cc65891aa55c62ad2'
                               ,'0x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                               )
                then 'OpenSea'
           end as platform
          ,case when a.zone in ('0xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                               ,'0x9b814233894cd227f561b78cc65891aa55c62ad2'
                               ,'0x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                               )
                then 3
           end as platform_version
          ,case when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_item_count as number_of_items
          ,'Trade' as evt_type
          ,a.original_amount / power(10, e1.decimals) * p1.price as usd_amount
          ,seller
          ,buyer
          ,a.original_amount / power(10, e1.decimals) as original_amount
          ,a.original_amount as original_amount_raw
          ,case when a.original_currency_contract = '0x0000000000000000000000000000000000000000' then 'ETH'
                else p1.symbol 
          end as original_currency
          ,a.original_currency_contract
          ,a.currency_contract
          ,a.nft_contract_address
          ,a.exchange_contract_address
          ,a.tx_hash
          ,a.block_number
          ,tx.`from` as tx_from
          ,tx.`to` as tx_to
          ,a.evt_index
          ,1 as trade_id
          ,a.fee_receive_address
          ,case when a.fee_currency_contract = '0x0000000000000000000000000000000000000000' then 'ETH'
                else p2.symbol 
          end as fee_currency
          ,a.fee_amount as fee_amount_raw
          ,a.fee_amount / power(10, e2.decimals) as fee_amount
          ,a.fee_amount / power(10, e2.decimals) * p2.price as fee_usd_amount
          ,a.zone as zone_address
          ,case when spc1.call_tx_hash is not null then 'Auction'
                when spc2.call_tx_hash is not null and spc2.parameters:basicOrderType::integer between 16 and 23 then 'Auction'
                when spc2.call_tx_hash is not null and spc2.parameters:basicOrderType::integer between 0 and 7 then 'Buy Now'
                when spc2.call_tx_hash is not null then 'Buy Now'
                when spc3.call_tx_hash is not null and spc3.advancedOrder:parameters:consideration[0]:identifierOrCriteria > '0' then 'Trait Offer'
                when spc3.call_tx_hash is not null then 'Collection Offer'
                else 'Private Sales'
           end as category          
          ,case when spc1.call_tx_hash is not null then 'Collection/Trait Offers' -- include English Auction and Dutch Auction
                when spc2.call_tx_hash is not null and spc2.parameters:basicOrderType::integer between 0 and 15 then 'Buy Now' -- Buy it directly
                when spc2.call_tx_hash is not null and spc2.parameters:basicOrderType::integer between 16 and 23 and spc2.parameters:considerationIdentifier = a.nft_token_id then 'Individual Offer'
                when spc2.call_tx_hash is not null then 'Buy Now'
                when spc3.call_tx_hash is not null and a.original_currency_contract = '0x0000000000000000000000000000000000000000' then 'Buy Now'
                when spc3.call_tx_hash is not null then 'Collection/Trait Offers' -- offer for collection
                when spc4.call_tx_hash is not null then 'Bulk Purchase' -- bundles of NFTs are purchased through aggregators or in a cart 
                when spc5.call_tx_hash is not null then 'Bulk Purchase' -- bundles of NFTs are purchased through aggregators or in a cart 
                when spc6.call_tx_hash is not null then 'Private Sales' -- sales for designated address
                else 'Buy Now'
           end as order_type  

      from iv_txn_level a
          left join {{ source('ethereum','transactions') }} tx on tx.hash = a.tx_hash
                                              and tx.block_number > 14801608
          left join {{ ref('tokens_ethereum_nft') }} n on n.contract_address = a.nft_contract_address
          left join {{ ref('tokens_ethereum_erc20') }} e1 on e1.contract_address = a.currency_contract
          left join {{ ref('tokens_ethereum_erc20') }} e2 on e2.contract_address = a.currency_contract2
          left join {{ source('prices', 'usd') }} p1 on p1.contract_address = a.currency_contract
                                  and p1.minute = date_trunc('minute', a.block_time)
                                  and p1.minute >= '2022-05-15'
                                  and p1.blockchain = 'ethereum'
          left join {{ source('prices', 'usd') }} p2 on p2.contract_address = a.currency_contract2
                                  and p2.minute = date_trunc('minute', a.block_time)
                                  and p2.minute >= '2022-05-15'
                                  and p2.blockchain = 'ethereum'
           left join {{ source('seaport_ethereum','Seaport_call_fulfillOrder') }} spc1 on spc1.call_tx_hash = a.tx_hash 
           left join {{ source('seaport_ethereum','Seaport_call_fulfillBasicOrder') }} spc2 on spc2.call_tx_hash = a.tx_hash
           left join {{ source('seaport_ethereum','Seaport_call_fulfillAdvancedOrder') }} spc3 on spc3.call_tx_hash = a.tx_hash
           left join {{ source('seaport_ethereum','Seaport_call_fulfillAvailableAdvancedOrders') }} spc4 on spc4.call_tx_hash = a.tx_hash
           left join {{ source('seaport_ethereum','Seaport_call_fulfillAvailableOrders') }} spc5 on spc5.call_tx_hash = a.tx_hash
           left join {{ source('seaport_ethereum','Seaport_call_matchOrders') }} spc6 on spc6.call_tx_hash = a.tx_hash
)
select block_time
      ,nft_project_name
      ,nft_token_id
      ,erc_standard
      ,platform
      ,platform_version
      ,trade_type
      ,number_of_items
      ,category
      ,evt_type
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
      ,evt_index
      ,trade_id
      ,fee_receive_address
      ,fee_currency
      ,fee_amount_raw
      ,fee_amount
      ,fee_usd_amount
      ,zone_address
  from iv_nft_trades
