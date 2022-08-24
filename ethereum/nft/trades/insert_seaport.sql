CREATE OR REPLACE FUNCTION nft.insert_seaport(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

with p1_call as (
    select 'basic_order' as main_type
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,max(parameters->>'basicOrderType') as order_type_id
      from seaport."Seaport_call_fulfillBasicOrder"
     where 1=1
       and call_block_time >= start_ts
       and call_block_time < end_ts
     group by 1,2,3,4
)
,p1_evt as (
    select c.main_type
          ,c.tx_hash
          ,c.block_time
          ,c.block_number
          ,c.order_type_id
          ,'offer' as sub_type
          ,offer_idx as sub_idx
          ,e.offerer as sender
          ,e.recipient as receiver
          ,e.zone
          ,concat('\x',substr(offer2->>'token',3,40))::bytea as token_contract_address 
          ,(offer2->>'amount')::numeric as original_amount
          ,offer2->>'itemType' as item_type
          ,(offer2->>'identifier') as token_id
          ,e.contract_address as exchange_contract_address
          ,e.evt_index
      from seaport."Seaport_evt_OrderFulfilled" e
           inner join p1_call c on c.tx_hash = e.evt_tx_hash
          ,jsonb_array_elements(offer) with ordinality as t (offer2, offer_idx)
     where 1=1
       and e.evt_block_time >= start_ts
       and e.evt_block_time < end_ts
    union all
    select c.main_type
          ,c.tx_hash
          ,c.block_time
          ,c.block_number
          ,c.order_type_id
          ,'consideration' as sub_type
          ,consideration_idx as sub_idx
          ,e.recipient as sender 
          ,concat('\x',substr(consideration2->>'recipient',3,40))::bytea as receiver
          ,e.zone 
          ,concat('\x',substr(consideration2->>'token',3,40))::bytea as token_contract_address 
          ,(consideration2->>'amount')::numeric as original_amount 
          ,consideration2->>'itemType' as item_type 
          ,(consideration2->>'identifier') as token_id
          ,e.contract_address as exchange_contract_address
          ,e.evt_index
      from seaport."Seaport_evt_OrderFulfilled" e
           inner join p1_call c on c.tx_hash = e.evt_tx_hash
          ,jsonb_array_elements(consideration) with ordinality as t (consideration2, consideration_idx)
      where e.evt_block_time >= start_ts
       and e.evt_block_time < end_ts
)
,p1_add_rn as (
    select (max(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                     when purchase_method = 'Buy Now' and sub_type = 'consideration' then token_contract_address::text
                end) over (partition by tx_hash, evt_index))::bytea as avg_original_currency_contract
          ,sum(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 1 then original_amount
                    when purchase_method = 'Buy Now' and sub_type = 'consideration' then original_amount
               end) over (partition by tx_hash, evt_index)
           / nft_transfer_count as avg_original_amount
          ,sum(case when fee_royalty_yn = 'fee' then original_amount end) over (partition by tx_hash, evt_index) / nft_transfer_count as avg_fee_amount
          ,sum(case when fee_royalty_yn = 'royalty' then original_amount end) over (partition by tx_hash, evt_index) / nft_transfer_count as avg_royalty_amount
          ,(max(case when fee_royalty_yn = 'fee' then receiver::text end) over (partition by tx_hash, evt_index))::bytea as avg_fee_receive_address
          ,(max(case when fee_royalty_yn = 'royalty' then receiver::text end) over (partition by tx_hash, evt_index))::bytea as avg_royalty_receive_address
          ,a.*
      from (select case when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 1 then 'fee'
                        when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 2 then 'royalty'
                        when purchase_method = 'Buy Now' and sub_type = 'consideration' and fee_royalty_idx = 2 then 'fee'
                        when purchase_method = 'Buy Now' and sub_type = 'consideration' and fee_royalty_idx = 3 then 'royalty'
                   end as fee_royalty_yn
                  ,case when purchase_method = 'Offer Accepted' and main_type = 'order' then 'Individual Offer'
                        when purchase_method = 'Offer Accepted' and main_type = 'basic_order' then 'Individual Offer'
                        when purchase_method = 'Offer Accepted' and main_type = 'advanced_order' then 'Collection/Trait Offers'
                        else 'Buy Now'
                   end as order_type
                  ,a.*
              from (select count(case when item_type in ('2','3') then 1 end) over (partition by tx_hash, evt_index) as nft_transfer_count
                          ,sum(case when item_type in ('0','1') then 1 end) over (partition by tx_hash, evt_index, sub_type order by sub_idx) as fee_royalty_idx
                          ,case when max(case when (sub_type,sub_idx,item_type) in (('offer',1,'1')) then 1 else 0 end) over (partition by tx_hash) = 1 then 'Offer Accepted'
                                else 'Buy Now'
                           end as purchase_method
                          ,a.*
                      from p1_evt a
                    ) a
             where nft_transfer_count > 0  -- some of trades without NFT happens in match_order
            ) a
)
,p1_txn_level as (
    select main_type
          ,sub_idx
          ,tx_hash
          ,block_time
          ,block_number
          ,zone
          ,exchange_contract_address
          ,evt_index
          ,order_type
          ,purchase_method
          ,receiver as buyer
          ,sender as seller
          ,avg_original_amount as original_amount
          ,avg_original_currency_contract as original_currency_contract
          ,avg_fee_receive_address as fee_receive_address
          ,avg_fee_amount as fee_amount
          ,avg_original_currency_contract as fee_currency_contract
          ,avg_royalty_receive_address as royalty_receive_address
          ,avg_royalty_amount as royalty_amount
          ,avg_original_currency_contract as royalty_currency_contract
          ,token_contract_address as nft_contract_address
          ,token_id as nft_token_id
          ,nft_transfer_count
          ,original_amount as nft_item_count 
          ,coalesce(avg_original_amount,0) + coalesce(avg_fee_amount,0) + coalesce(avg_royalty_amount,0) as attempt_amount
          ,0 as revert_amount
          ,false as reverted
          ,case when nft_transfer_count > 1 then true else false end as price_estimated
          ,'' as offer_order_type
          ,item_type
          ,order_type_id
      from p1_add_rn a
     where item_type in ('2','3')
)
,p1_nft_trades as (
    select a.block_time
          ,a.nft_contract_address
          ,n.name as nft_project_name
          ,nft_token_id
          ,case when item_type = '2' then 'erc721'
                when item_type = '3' then 'erc1155'
           end as erc_standard 
          ,order_type
          ,purchase_method
          ,case when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_item_count 
          ,seller
          ,buyer
          ,a.original_currency_contract
          ,case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else a.original_currency_contract
           end as currency_contract
          ,case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t1.symbol
           end as original_currency
          ,a.original_amount / 10^t1.decimals as original_amount
          ,a.original_amount as original_amount_raw
          ,a.original_amount / 10^t1.decimals * p1.price as usd_amount
          ,a.fee_receive_address
        --   ,case when fee_amount > 0 and a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
        --         when fee_amount > 0 then t1.symbol
        --   end as fee_currency
          ,a.fee_amount / 10^t1.decimals as fee_amount
          ,a.fee_amount as fee_amount_raw
          ,a.fee_amount / 10^t1.decimals * p1.price as fee_usd_amount
          ,a.royalty_receive_address
        --   ,case when royalty_amount > 0 and a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
        --         when royalty_amount > 0 then t1.symbol
        --   end as royalty_currency
          ,a.royalty_amount / 10^t1.decimals as royalty_amount
          ,a.royalty_amount as royalty_amount_raw
          ,a.royalty_amount / 10^t1.decimals * p1.price as royalty_usd_amount
        --   ,attempt_amount  / 10^t1.decimals as attempt_amount
        --   ,revert_amount / 10^t1.decimals as revert_amount
        --   ,reverted
          ,price_estimated
          ,a.exchange_contract_address
          ,a.zone as zone_address
          ,case when a.zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                              ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'
                              ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                              ,'\x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd'
                              )
                then 'OpenSea'
           end as platform
          ,a.block_number
          ,a.tx_hash
          ,tx."from" as tx_from
          ,tx."to" as tx_to
          ,row_number () over (partition by tx_hash order by sub_idx) as trade_id
          ,main_type as call_function
          ,order_type_id
          ,NULL::text as param1
          ,NULL::text as param2
          ,NULL::text as param3
      from p1_txn_level a
          left join ethereum.transactions tx on tx.hash = a.tx_hash
                                              and tx.block_number > 14801608
                                            and tx.block_time >= start_ts
                                            and tx.block_time < end_ts
          left join nft.tokens n on n.contract_address = a.nft_contract_address
          left join erc20.tokens t1 on t1.contract_address = case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                  else a.original_currency_contract
                                                              end                                
          left join prices.usd p1 on p1.contract_address = case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                 else a.original_currency_contract
                                                            end
                                  and p1.minute = date_trunc('minute', a.block_time)
                                  and p1.minute >= start_ts
                                  and p1.minute < end_ts
)
,p2_call as (
    select 'available_advanced_orders' as main_type
          ,'bulk' as sub_type
          ,t.idx as sub_idx
          ,t.each->'parameters'->>'zone' as zone
          ,t.each->'parameters'->>'offerer' as offerer 
          ,t.each->'parameters'->'offer'->0->>'token' as offer_token
          ,t.each->'parameters'->'offer'->0->>'itemType' as offer_item_type
          ,t.each->'parameters'->'offer'->0->>'identifierOrCriteria' as offer_identifier
          ,t.each->'parameters'->>'orderType' as offer_order_type
          ,t.each->'parameters'->'consideration'->0->>'token' as price_token
          ,t.each->'parameters'->'consideration'->0->>'itemType' as price_item_type
          ,(t.each->'parameters'->'consideration'->0->>'startAmount')::numeric as price_amount
          ,(t.each->'parameters'->'consideration'->1->>'startAmount')::numeric as fee_amount
          ,(t.each->'parameters'->'consideration'->2->>'startAmount')::numeric as royalty_amount
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
      from seaport."Seaport_call_fulfillAvailableAdvancedOrders" c
          ,jsonb_array_elements("advancedOrders") with ordinality as t (each, idx)
     where call_success
       and call_block_time >= start_ts
       and call_block_time < end_ts
    union all
    select 'available_orders' as main_type
          ,'bulk' as sub_type
          ,t.idx as sub_idx
          ,t.each->'parameters'->>'zone' as zone
          ,t.each->'parameters'->>'offerer' as offerer 
          ,t.each->'parameters'->'offer'->0->>'token' as offer_token
          ,t.each->'parameters'->'offer'->0->>'itemType' as offer_item_type
          ,t.each->'parameters'->'offer'->0->>'identifierOrCriteria' as offer_identifier
          ,t.each->'parameters'->>'orderType' as offer_order_type
          ,t.each->'parameters'->'consideration'->0->>'token' as price_token
          ,t.each->'parameters'->'consideration'->0->>'itemType' as price_item_type
          ,(t.each->'parameters'->'consideration'->0->>'startAmount')::numeric as price_amount
          ,(t.each->'parameters'->'consideration'->1->>'startAmount')::numeric as fee_amount
          ,(t.each->'parameters'->'consideration'->2->>'startAmount')::numeric as royalty_amount
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
      from seaport."Seaport_call_fulfillAvailableOrders" c
          ,jsonb_array_elements(orders) with ordinality as t (each, idx)
     where call_success     
       and call_block_time >= start_ts
       and call_block_time < end_ts
)
,p2_evt as (
    select c.*
          ,e.evt_tx_hash
          ,e.recipient::text as recipient
          ,(e.offer->0->>'amount')::numeric as evt_token_amount
          ,e.consideration->0->>'token' as evt_price_token
          ,(e.consideration->0->>'amount')::numeric as evt_price_amount
          ,e.consideration->0->>'itemType' as evt_price_item_type
          ,e.consideration->0->>'recipient' as evt_price_recipient
          ,e.consideration->0->>'identifier' as evt_price_identifier
          ,e.consideration->1->>'token' as evt_fee_token
          ,(e.consideration->1->>'amount')::numeric as evt_fee_amount
          ,e.consideration->1->>'itemType' as evt_fee_item_type
          ,e.consideration->1->>'recipient' as evt_fee_recipient
          ,e.consideration->1->>'identifier' as evt_fee_identifier
          ,e.consideration->2->>'token' as evt_royalty_token
          ,(e.consideration->2->>'amount')::numeric as evt_royalty_amount
          ,e.consideration->2->>'itemType' as evt_royalty_item_type
          ,e.consideration->2->>'recipient' as evt_royalty_recipient
          ,e.consideration->2->>'identifier' as evt_royalty_identifier
          ,e.evt_index
      from p2_call c
           inner join seaport."Seaport_evt_OrderFulfilled" e on e.evt_tx_hash = c.tx_hash
                                                            and e.evt_block_time >= start_ts
                                                            and e.evt_block_time < end_ts
                                                            and e.offerer = concat('\x',substr(c.offerer,3,40))::bytea
                                                            and e.offer->0->>'token' = c.offer_token
                                                            and e.offer->0->>'identifier' = c.offer_identifier
                                                            and e.offer->0->>'itemType' = c.offer_item_type
)
,p2_transfer_level as (
    select a.main_type
          ,a.sub_idx
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.zone
          ,a.exchange_contract_address
          ,offer_token as nft_address
          ,offer_identifier as nft_token_id
          ,recipient as buyer
          ,offerer as seller
          ,offer_item_type as offer_item_type
          ,offer_order_type as offer_order_type
          ,offer_identifier as nft_token_id_dcnt
          ,price_token as price_token
          ,price_item_type as price_item_type
          ,price_amount as price_amount
          ,fee_amount as fee_amount
          ,royalty_amount as royalty_amount
          ,evt_token_amount as evt_token_amount
          ,evt_price_amount as evt_price_amount
          ,evt_fee_amount as evt_fee_amount
          ,evt_royalty_amount as evt_royalty_amount
          ,evt_fee_token as evt_fee_token
          ,evt_royalty_token as evt_royalty_token
          ,evt_fee_recipient as evt_fee_recipient
          ,evt_royalty_recipient as evt_royalty_recipient
          ,coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) as attempt_amount
          ,case when evt_tx_hash is not null then coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) end as trade_amount
          ,case when evt_tx_hash is null then coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) else 0 end as revert_amount
          ,case when evt_tx_hash is null then true else false end as reverted
          ,'Bulk Purchase' as trade_type
          ,'Bulk Purchase' as order_type
          ,'Buy Now' as purchase_method
      from p2_evt a
)
,p2_nft_trades as ( 
    select a.block_time
          ,concat('\x',substr(a.nft_address,3,40))::bytea as nft_contract_address 
          ,n.name as nft_project_name 
          ,a.nft_token_id as nft_token_id
          ,case when offer_item_type = '2' then 'erc721'
                when offer_item_type = '3' then 'erc1155'
           end as erc_standard 
          ,order_type
          ,purchase_method
          ,trade_type
          ,evt_token_amount as nft_item_count
          ,concat('\x',substr(seller,3,40))::bytea as seller 
          ,concat('\x',substr(buyer,3,40))::bytea as buyer
          ,concat('\x',substr(a.price_token,3,40))::bytea as original_currency_contract
          ,case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else concat('\x',substr(a.price_token,3,40))::bytea
           end as currency_contract
          ,case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t1.symbol
           end as original_currency 
          ,a.trade_amount / 10^t1.decimals as original_amount 
          ,a.trade_amount as original_amount_raw
          ,a.trade_amount / 10^t1.decimals * p1.price as usd_amount 
          ,case when evt_fee_amount > 0 then concat('\x',substr(evt_fee_recipient,3,40))::bytea end as fee_receive_address
        --   ,case when evt_fee_amount > 0 and concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
        --         when evt_fee_amount > 0 then t1.symbol
        --   end as fee_currency 
          ,a.evt_fee_amount / 10^t1.decimals as fee_amount
          ,a.evt_fee_amount as fee_amount_raw
          ,a.evt_fee_amount / 10^t1.decimals * p1.price as fee_usd_amount
          ,case when evt_royalty_amount > 0 then concat('\x',substr(evt_royalty_recipient,3,40))::bytea end as royalty_receive_address
        --   ,case when evt_royalty_amount > 0 and concat('\x',substr(a.evt_royalty_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
        --         when evt_royalty_amount > 0 then t1.symbol
        --   end as royalty_currency 
          ,a.evt_royalty_amount / 10^t1.decimals as royalty_amount
          ,a.evt_royalty_amount as royalty_amount_raw
          ,a.evt_royalty_amount / 10^t1.decimals * p1.price as royalty_usd_amount
        --   ,attempt_amount  / 10^t1.decimals as attempt_amount
        --   ,revert_amount / 10^t1.decimals as revert_amount
        --   ,reverted
          ,false as price_estimated
          ,a.exchange_contract_address
          ,concat('\x',substr(a.zone,3,40))::bytea as zone_address
          ,case when a.zone in ('0xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                              ,'0x9b814233894cd227f561b78cc65891aa55c62ad2'
                              ,'0x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                              ,'0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd'
                              )
                then 'OpenSea' 
           end as platform
          ,a.block_number
          ,a.tx_hash
          ,tx."from" as tx_from
          ,tx."to" as tx_to
          ,row_number () over (partition by tx_hash order by sub_idx) as trade_id
          ,main_type as call_function
          ,offer_order_type as order_type_id  -- tobe
          ,NULL::text as param1   -- tobe
          ,NULL::text as param2   -- tobe
          ,NULL::text as param3   -- tobe
      from p2_transfer_level a
          left join ethereum.transactions tx on tx.hash = a.tx_hash 
                                              and tx.block_number > 14801608
                                            and tx.block_time >= start_ts
                                            and tx.block_time < end_ts
          left join nft.tokens n on n.contract_address = concat('\x',substr(a.nft_address,3,40))::bytea
          left join erc20.tokens t1 on t1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                  else concat('\x',substr(a.price_token,3,40))::bytea
                                                              end                                
          left join prices.usd p1 on p1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                 else concat('\x',substr(a.price_token,3,40))::bytea
                                                            end
                                  and p1.minute = date_trunc('minute', a.block_time)
                                  and p1.minute >= start_ts
                                  and p1.minute < end_ts
)
,p3_call as (
    select 'order' as main_type
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,max("order"->'parameters'->>'orderType') as order_type_id
      from seaport."Seaport_call_fulfillOrder"
     where 1=1      
       and call_block_time >= start_ts
       and call_block_time < end_ts
     group by 1,2,3,4
     union all
    select 'advanced_order' as main_type
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,max("advancedOrder"->'parameters'->>'orderType') as order_type_id
      from seaport."Seaport_call_fulfillAdvancedOrder"
     where 1=1      
       and call_block_time >= start_ts
       and call_block_time < end_ts
     group by 1,2,3,4
)      
,p3_evt as (
    select c.main_type
          ,c.tx_hash
          ,c.block_time
          ,c.block_number
          ,c.order_type_id
          ,'offer' as sub_type
          ,offer_idx as sub_idx
          ,e.offerer as sender
          ,e.recipient as receiver
          ,e.zone
          ,concat('\x',substr(offer2->>'token',3,40))::bytea as token_contract_address 
          ,(offer2->>'amount')::numeric as original_amount
          ,offer2->>'itemType' as item_type
          ,(offer2->>'identifier') as token_id
          ,e.contract_address as exchange_contract_address
          ,e.evt_index
      from seaport."Seaport_evt_OrderFulfilled" e
           inner join p3_call c on c.tx_hash = e.evt_tx_hash
          ,jsonb_array_elements(offer) with ordinality as t (offer2, offer_idx)
     where 1=1
       and e.evt_block_time >= start_ts
       and e.evt_block_time < end_ts
    union all
    select c.main_type
          ,c.tx_hash
          ,c.block_time
          ,c.block_number
          ,c.order_type_id
          ,'consideration' as sub_type
          ,consideration_idx as sub_idx
          ,e.recipient as sender 
          ,concat('\x',substr(consideration2->>'recipient',3,40))::bytea as receiver
          ,e.zone 
          ,concat('\x',substr(consideration2->>'token',3,40))::bytea as token_contract_address 
          ,(consideration2->>'amount')::numeric as original_amount 
          ,consideration2->>'itemType' as item_type 
          ,(consideration2->>'identifier') as token_id
          ,e.contract_address as exchange_contract_address
          ,e.evt_index
      from seaport."Seaport_evt_OrderFulfilled" e
           inner join p3_call c on c.tx_hash = e.evt_tx_hash
          ,jsonb_array_elements(consideration) with ordinality as t (consideration2, consideration_idx)
      where e.evt_block_time >= start_ts
       and e.evt_block_time < end_ts
)
,p3_add_rn as (
    select (max(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                     when purchase_method = 'Buy Now' and sub_type = 'consideration' then token_contract_address::text
                end) over (partition by tx_hash, evt_index))::bytea as avg_original_currency_contract
          ,sum(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 1 then original_amount
                    when purchase_method = 'Buy Now' and sub_type = 'consideration' then original_amount
               end) over (partition by tx_hash, evt_index)
           / nft_transfer_count as avg_original_amount
          ,sum(case when fee_royalty_yn = 'fee' then original_amount end) over (partition by tx_hash, evt_index) / nft_transfer_count as avg_fee_amount
          ,sum(case when fee_royalty_yn = 'royalty' then original_amount end) over (partition by tx_hash, evt_index) / nft_transfer_count as avg_royalty_amount
          ,(max(case when fee_royalty_yn = 'fee' then receiver::text end) over (partition by tx_hash, evt_index))::bytea as avg_fee_receive_address
          ,(max(case when fee_royalty_yn = 'royalty' then receiver::text end) over (partition by tx_hash, evt_index))::bytea as avg_royalty_receive_address
          ,a.*
      from (select case when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 1 then 'fee'
                        when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 2 then 'royalty'
                        when purchase_method = 'Buy Now' and sub_type = 'consideration' and fee_royalty_idx = 2 then 'fee'
                        when purchase_method = 'Buy Now' and sub_type = 'consideration' and fee_royalty_idx = 3 then 'royalty'
                   end as fee_royalty_yn
                  ,case when purchase_method = 'Offer Accepted' and main_type = 'order' then 'Individual Offer'
                        when purchase_method = 'Offer Accepted' and main_type = 'basic_order' then 'Individual Offer'
                        when purchase_method = 'Offer Accepted' and main_type = 'advanced_order' then 'Collection/Trait Offers'
                        else 'Buy Now'
                   end as order_type
                  ,a.*
              from (select count(case when item_type in ('2','3') then 1 end) over (partition by tx_hash, evt_index) as nft_transfer_count
                          ,sum(case when item_type in ('0','1') then 1 end) over (partition by tx_hash, evt_index, sub_type order by sub_idx) as fee_royalty_idx
                          ,case when max(case when (sub_type,sub_idx,item_type) in (('offer',1,'1')) then 1 else 0 end) over (partition by tx_hash) = 1 then 'Offer Accepted'
                                else 'Buy Now'
                           end as purchase_method
                          ,a.*
                      from p3_evt a
                    ) a
             where nft_transfer_count > 0  -- some of trades without NFT happens in match_order
            ) a
)
,p3_txn_level as (
    select main_type
          ,sub_idx
          ,tx_hash
          ,block_time
          ,block_number
          ,zone
          ,exchange_contract_address
          ,evt_index
          ,order_type
          ,purchase_method
          ,receiver as buyer
          ,sender as seller
          ,avg_original_amount as original_amount
          ,avg_original_currency_contract as original_currency_contract
          ,avg_fee_receive_address as fee_receive_address
          ,avg_fee_amount as fee_amount
          ,avg_original_currency_contract as fee_currency_contract
          ,avg_royalty_receive_address as royalty_receive_address
          ,avg_royalty_amount as royalty_amount
          ,avg_original_currency_contract as royalty_currency_contract
          ,token_contract_address as nft_contract_address
          ,token_id as nft_token_id
          ,nft_transfer_count
          ,original_amount as nft_item_count 
          ,coalesce(avg_original_amount,0) + coalesce(avg_fee_amount,0) + coalesce(avg_royalty_amount,0) as attempt_amount
          ,0 as revert_amount
          ,false as reverted
          ,case when nft_transfer_count > 1 then true else false end as price_estimated
          ,'' as offer_order_type
          ,item_type
          ,order_type_id
      from p3_add_rn a
     where item_type in ('2','3')
)
,p3_nft_trades as (
    select a.block_time
          ,a.nft_contract_address
          ,n.name as nft_project_name
          ,nft_token_id
          ,case when item_type = '2' then 'erc721'
                when item_type = '3' then 'erc1155'
           end as erc_standard 
          ,order_type
          ,purchase_method
          ,case when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_item_count 
          ,seller
          ,buyer
          ,a.original_currency_contract
          ,case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else a.original_currency_contract
           end as currency_contract
          ,case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t1.symbol
           end as original_currency
          ,a.original_amount / 10^t1.decimals as original_amount
          ,a.original_amount as original_amount_raw
          ,a.original_amount / 10^t1.decimals * p1.price as usd_amount
          ,a.fee_receive_address
        --   ,case when fee_amount > 0 and a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
        --         when fee_amount > 0 then t1.symbol
        --   end as fee_currency
          ,a.fee_amount / 10^t1.decimals as fee_amount
          ,a.fee_amount as fee_amount_raw
          ,a.fee_amount / 10^t1.decimals * p1.price as fee_usd_amount
          ,a.royalty_receive_address
        --   ,case when royalty_amount > 0 and a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
        --         when royalty_amount > 0 then t1.symbol
        --   end as royalty_currency
          ,a.royalty_amount / 10^t1.decimals as royalty_amount
          ,a.royalty_amount as royalty_amount_raw
          ,a.royalty_amount / 10^t1.decimals * p1.price as royalty_usd_amount
        --   ,attempt_amount  / 10^t1.decimals as attempt_amount
        --   ,revert_amount / 10^t1.decimals as revert_amount
        --   ,reverted
          ,price_estimated
          ,a.exchange_contract_address
          ,a.zone as zone_address
          ,case when a.zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                              ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'
                              ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                              ,'\x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd'
                              )
                then 'OpenSea'
           end as platform
          ,a.block_number
          ,a.tx_hash
          ,tx."from" as tx_from
          ,tx."to" as tx_to
          ,row_number () over (partition by tx_hash order by sub_idx) as trade_id
          ,main_type as call_function
          ,order_type_id  -- tobe
          ,NULL::text as param1   -- tobe
          ,NULL::text as param2   -- tobe
          ,NULL::text as param3   -- tobe
      from p3_txn_level a
          left join ethereum.transactions tx on tx.hash = a.tx_hash
                                              and tx.block_number > 14801608
                                            and tx.block_time >= start_ts
                                            and tx.block_time < end_ts
          left join nft.tokens n on n.contract_address = a.nft_contract_address
          left join erc20.tokens t1 on t1.contract_address = case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                  else a.original_currency_contract
                                                              end                                
          left join prices.usd p1 on p1.contract_address = case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                 else a.original_currency_contract
                                                            end
                                  and p1.minute = date_trunc('minute', a.block_time)
                                  and p1.minute >= start_ts
                                  and p1.minute < end_ts
)
,p4_call as (
    select 'match_orders' as main_type
          ,'match_orders' as sub_type
          ,t.idx as sub_idx
          ,c.orders->0->'parameters'->>'zone' as zone
          ,t.each->>'offerer' as offerer 
          ,t.each->'item'->>'token' as offer_token
          ,(t.each->'item'->>'amount')::numeric as offer_amount
          ,t.each->'item'->>'itemType' as offer_item_type
          ,t.each->'item'->>'identifier' as offer_identifier
          ,t.each->'item'->>'recipient' as recipient
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
      from seaport."Seaport_call_matchOrders" c
          ,jsonb_array_elements("output_executions") with ordinality as t (each, idx)
     where call_success
       and call_block_time >= start_ts
       and call_block_time < end_ts
    union all
    select 'match_advanced_orders' as main_type
          ,'match_advanced_orders' as sub_type
          ,t.idx as sub_idx
          ,c."advancedOrders"->0->'parameters'->>'zone' as zone
          ,t.each->>'offerer' as offerer 
          ,t.each->'item'->>'token' as offer_token
          ,(t.each->'item'->>'amount')::numeric as offer_amount
          ,t.each->'item'->>'itemType' as offer_item_type
          ,t.each->'item'->>'identifier' as offer_identifier
          ,t.each->'item'->>'recipient' as recipient
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
      from seaport."Seaport_call_matchAdvancedOrders" c
          ,jsonb_array_elements("output_executions") with ordinality as t (each, idx)
     where call_success
       and call_block_time >= start_ts
       and call_block_time < end_ts
)
,p4_add_rn as (
    select max(case when fee_royalty_yn = 'price' then offerer end) over (partition by tx_hash) as price_offerer
          ,max(case when fee_royalty_yn = 'price' then recipient end) over (partition by tx_hash) as price_recipient
          ,max(case when fee_royalty_yn = 'price' then offer_token end) over (partition by tx_hash) as price_token
          ,max(case when fee_royalty_yn = 'price' then offer_amount end) over (partition by tx_hash) / nft_transfer_count as price_amount
          ,max(case when fee_royalty_yn = 'price' then offer_item_type end) over (partition by tx_hash) as price_item_type
          ,max(case when fee_royalty_yn = 'price' then offer_identifier end) over (partition by tx_hash) as price_id
          ,max(case when fee_royalty_yn = 'fee' then offerer end) over (partition by tx_hash) as fee_offerer
          ,max(case when fee_royalty_yn = 'fee' then recipient end) over (partition by tx_hash) as fee_recipient
          ,max(case when fee_royalty_yn = 'fee' then offer_token end) over (partition by tx_hash) as fee_token
          ,max(case when fee_royalty_yn = 'fee' then offer_amount end) over (partition by tx_hash) / nft_transfer_count as fee_amount
          ,max(case when fee_royalty_yn = 'fee' then offer_item_type end) over (partition by tx_hash) as fee_item_type
          ,max(case when fee_royalty_yn = 'fee' then offer_identifier end) over (partition by tx_hash) as fee_id
          ,max(case when fee_royalty_yn = 'royalty' then offerer end) over (partition by tx_hash) as royalty_offerer
          ,max(case when fee_royalty_yn = 'royalty' then recipient end) over (partition by tx_hash) as royalty_recipient
          ,max(case when fee_royalty_yn = 'royalty' then offer_token end) over (partition by tx_hash) as royalty_token
          ,max(case when fee_royalty_yn = 'royalty' then offer_amount end) over (partition by tx_hash) / nft_transfer_count as royalty_amount
          ,max(case when fee_royalty_yn = 'royalty' then offer_item_type end) over (partition by tx_hash) as royalty_item_type
          ,max(case when fee_royalty_yn = 'royalty' then offer_identifier end) over (partition by tx_hash) as royalty_id
          ,a.*
      from (select case when fee_royalty_idx = 1 then 'price'
                        when fee_royalty_idx = 2 then 'fee'
                        when fee_royalty_idx = 3 then 'royalty'
                   end as fee_royalty_yn
                  ,a.*
              from (select count(case when offer_item_type in ('2','3') then 1 end) over (partition by tx_hash) as nft_transfer_count
                          ,sum(case when offer_item_type in ('0','1') then 1 end) over (partition by tx_hash order by sub_idx) as fee_royalty_idx
                          ,a.*
                      from p4_call a
                    ) a
             where nft_transfer_count > 0  -- some of trades without NFT happens in match_order
            ) a
)
,p4_transfer_level as (
    select a.main_type
          ,a.sub_idx
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.zone
          ,a.exchange_contract_address
          ,offer_token as nft_address
          ,offer_identifier as nft_token_id
          ,recipient as buyer
          ,offerer as seller
          ,offer_item_type as offer_item_type
          ,offer_identifier as nft_token_id_dcnt
          ,offer_amount as nft_token_amount
          ,price_token as price_token
          ,price_item_type as price_item_type
          ,price_amount as price_amount
          ,fee_amount as fee_amount
          ,royalty_amount as royalty_amount
          ,price_amount as evt_price_amount
          ,fee_amount as evt_fee_amount
          ,royalty_amount as evt_royalty_amount
          ,fee_token as evt_fee_token
          ,royalty_token as evt_royalty_token
          ,fee_recipient as evt_fee_recipient
          ,royalty_recipient as evt_royalty_recipient
          ,coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) as attempt_amount
          ,0 as revert_amount
          ,false as reverted
          ,'' as offer_order_type
          ,'Private Sales' as order_type
          ,'Buy Now' as purchase_method
          ,nft_transfer_count
      from p4_add_rn a
     where offer_item_type in ('2','3')
)
,p4_nft_trades as ( 
    select a.block_time
          ,concat('\x',substr(a.nft_address,3,40))::bytea as nft_contract_address 
          ,n.name as nft_project_name 
          ,a.nft_token_id as nft_token_id
          ,case when offer_item_type = '2' then 'erc721'
                when offer_item_type = '3' then 'erc1155'
           end as erc_standard 
          ,a.order_type
          ,a.purchase_method
          ,case when order_type = 'Bulk Purchase' then 'Bulk Purchase'
                when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_token_amount as nft_item_count
          ,concat('\x',substr(seller,3,40))::bytea as seller 
          ,concat('\x',substr(buyer,3,40))::bytea as buyer
          ,concat('\x',substr(a.price_token,3,40))::bytea as original_currency_contract
          ,case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else concat('\x',substr(a.price_token,3,40))::bytea
           end as currency_contract
          ,case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t1.symbol
           end as original_currency 
          ,a.attempt_amount / 10^t1.decimals as original_amount 
          ,a.attempt_amount as original_amount_raw 
          ,a.attempt_amount / 10^t1.decimals * p1.price as usd_amount 
          ,case when evt_fee_amount > 0 then concat('\x',substr(evt_fee_recipient,3,40))::bytea end as fee_receive_address
        --   ,case when evt_fee_amount > 0 and concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
        --         when evt_fee_amount > 0 then t1.symbol
        --   end as fee_currency 
          ,a.evt_fee_amount / 10^t1.decimals as fee_amount
          ,a.evt_fee_amount as fee_amount_raw
          ,a.evt_fee_amount / 10^t1.decimals * p1.price as fee_usd_amount
          ,case when evt_royalty_amount > 0 then concat('\x',substr(evt_royalty_recipient,3,40))::bytea end as royalty_receive_address
        --   ,case when evt_royalty_amount > 0 and concat('\x',substr(a.evt_royalty_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
        --         when evt_royalty_amount > 0 then t1.symbol
        --   end as royalty_currency 
          ,a.evt_royalty_amount / 10^t1.decimals as royalty_amount
          ,a.evt_royalty_amount as royalty_amount_raw
          ,a.evt_royalty_amount / 10^t1.decimals * p1.price as royalty_usd_amount
        --   ,attempt_amount  / 10^t1.decimals as attempt_amount
        --   ,revert_amount / 10^t1.decimals as revert_amount
        --   ,reverted
          ,false as price_estimated
          ,a.exchange_contract_address
          ,concat('\x',substr(a.zone,3,40))::bytea as zone_address
          ,case when a.zone in ('0xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                              ,'0x9b814233894cd227f561b78cc65891aa55c62ad2'
                              ,'0x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                              ,'0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd'
                              )
                then 'OpenSea' 
           end as platform
          ,a.block_number
          ,a.tx_hash
          ,tx."from" as tx_from
          ,tx."to" as tx_to
          ,row_number () over (partition by tx_hash order by sub_idx) as trade_id
          ,main_type as call_function
          ,offer_order_type as order_type_id  -- tobe
          ,NULL::text as param1   -- tobe
          ,NULL::text as param2   -- tobe
          ,NULL::text as param3   -- tobe
      from p4_transfer_level a
          left join ethereum.transactions tx on tx.hash = a.tx_hash 
                                              and tx.block_number > 14801608
                                            and tx.block_time >= start_ts
                                            and tx.block_time < end_ts
          left join nft.tokens n on n.contract_address = concat('\x',substr(a.nft_address,3,40))::bytea
          left join erc20.tokens t1 on t1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                  else concat('\x',substr(a.price_token,3,40))::bytea
                                                              end                                
          left join prices.usd p1 on p1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                 else concat('\x',substr(a.price_token,3,40))::bytea
                                                            end
                                  and p1.minute = date_trunc('minute', a.block_time)
                                  and p1.minute >= start_ts
                                  and p1.minute < end_ts
)
,iv_nft_trades as (
    select *
      from p1_nft_trades
    union all
    select *
      from p2_nft_trades
    union all
    select *
      from p3_nft_trades
    union all
    select *
      from p4_nft_trades
)
,rows AS (
    INSERT INTO nft.trades (
            block_time,
            nft_project_name,
            nft_token_id,
            erc_standard,
            platform,
            platform_version,
            trade_type,
            number_of_items,
            category,
            evt_type,
            usd_amount,
            seller,
            buyer,
            original_amount,
            original_amount_raw,
            original_currency,
            original_currency_contract,
            currency_contract,
            nft_contract_address,
            exchange_contract_address,
            tx_hash,
            block_number,
            nft_token_ids_array,
            senders_array,
            recipients_array,
            erc_types_array,
            nft_contract_addresses_array,
            erc_values_array,
            tx_from,
            tx_to,
            trace_address,
            evt_index,
            trade_id
    )
    select block_time 
            ,nft_project_name
            ,nft_token_id 
            ,erc_standard
            ,platform
            ,'3' as platform_version  
            ,trade_type
            ,nft_item_count as number_of_items 
            ,purchase_method as category
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
            ,NULL::text[] as nft_token_ids_array
            ,NULL::bytea[] as senders_array
            ,NULL::bytea[] as recipients_array
            ,NULL::text[] as erc_types_array
            ,NULL::bytea[] as nft_contract_addresses_array
            ,NULL::numeric[] as erc_values_array
            ,tx_from 
            ,tx_to
            ,NULL::numeric[] as trace_address
            ,0 as evt_index
            ,trade_id
      from iv_nft_trades
     where platform = 'OpenSea' -- seaport contract has other than OpenSea, so we have to exclude them.
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2022
SELECT nft.insert_seaport(
    '2022-06-10'
    , NOW()
    , (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2022-06-10')
    , (SELECT MAX(number) FROM ethereum.blocks WHERE time < NOW() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2022-06-10'
    AND block_time <= NOW() - interval '20 minutes'
    AND platform = 'OpenSea'
    AND platform_version = '3'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/15 * * * *', $$
    SELECT nft.insert_seaport(
        (SELECT MAX(block_time) - interval '6 hours' FROM nft.trades WHERE platform='OpenSea' AND platform_version = '3')
        , (SELECT NOW() - interval '20 minutes')
        , (SELECT MAX(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='OpenSea' AND platform_version = '3'))
        , (SELECT MAX(number) FROM ethereum.blocks WHERE time < NOW() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule; 
