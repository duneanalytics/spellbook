CREATE OR REPLACE FUNCTION seaport.insert_transactions (p_start_ts timestamptz, p_end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

-- ## this function consists of 4 parts,
-- ## p1_ : seaport."Seaport_call_fulfillBasicOrder"
-- ## p2_ : seaport."Seaport_call_fulfillAvailableAdvancedOrders"
-- ##       seaport."Seaport_call_fulfillAvailableOrders"
-- ## p3_ : seaport."Seaport_call_fulfillOrder"
-- ##       seaport."Seaport_call_fulfillAdvancedOrder"
-- ## p4_ : seaport."Seaport_call_matchOrders"
-- ##     : seaport."Seaport_call_matchAdvancedOrders"

with p1_call as (
    select 'basic_order' as main_type
          ,'single' as sub_type
          ,coalesce(call_trace_address[1],1) as sub_idx -- some basic_order has been occurred multiple at one transaction, and call_trace_address can distinguish them so I used this as sub_idx
          ,parameters->>'zone' as zone
          ,parameters->>'offerer' as offerer
          ,parameters->>'offerToken' as offer_token
          ,(parameters->>'offerAmount')::numeric as offer_amount
          ,case when (parameters->>'basicOrderType')::numeric in (0,1,2,3,8,9,10,11) then '2'
                when (parameters->>'basicOrderType')::numeric in (4,5,6,7,12,13,14,15) then '3'
                when (parameters->>'basicOrderType')::numeric < 24 then '1'
           end as offer_item_type
          ,parameters->>'offerIdentifier' as offer_identifier
          ,parameters->>'basicOrderType' as order_type_id
          ,parameters->>'considerationToken' as consideration_token
          ,(parameters->>'considerationAmount')::numeric as consideration_amount
          ,case when (parameters->>'basicOrderType')::numeric < 8 then '0'
                when (parameters->>'basicOrderType')::numeric < 16 then '1'
                when (parameters->>'basicOrderType')::numeric < 20 then '2'
                when (parameters->>'basicOrderType')::numeric < 24 then '3'
           end as consideration_item_type
          ,(parameters->'additionalRecipients'->0->>'amount')::numeric as fee_amount
          ,parameters->'additionalRecipients'->0->>'recipient' as fee_recipient
          ,(parameters->'additionalRecipients'->1->>'amount')::numeric as royalty_amount
          ,parameters->'additionalRecipients'->1->>'recipient' as royalty_recipient
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
          ,case when (parameters->>'basicOrderType')::numeric < 16 then 'Buy Now'
                else 'Offer Accepted'
           end as purchase_method
      from seaport."Seaport_call_fulfillBasicOrder" c
     where call_success
       and call_block_time >= p_start_ts
       and call_block_time < p_end_ts     
)
,p1_evt as (
    select c.*
          ,e.evt_tx_hash
          ,e.recipient::text as recipient
          ,e.offer->0->>'token' as evt_offer_token
          ,(e.offer->0->>'amount')::numeric as evt_offer_amount
          ,e.offer->0->>'itemType' as evt_offer_item_type
          ,e.offer->0->>'identifier' as evt_offer_identifier
          ,e.consideration->0->>'token' as evt_consideration_token
          ,(e.consideration->0->>'amount')::numeric as evt_consideration_amount
          ,e.consideration->0->>'itemType' as evt_consideration_item_type
          ,e.consideration->0->>'recipient' as evt_consideration_recipient
          ,e.consideration->0->>'identifier' as evt_consideration_identifier
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
      from p1_call c
           inner join seaport."Seaport_evt_OrderFulfilled" e on e.evt_tx_hash = c.tx_hash
           -- if we want to get reverted trades, then do left join
                                                            and e.offerer = concat('\x',substr(c.offerer,3,40))::bytea 
                                                            and e.offer->0->>'token' = c.offer_token
                                                            and e.offer->0->>'identifier' = c.offer_identifier
                                                            and e.offer->0->>'itemType' = c.offer_item_type
)
,p1_evt_add as (
    select tx_hash as txid
          ,case when purchase_method = 'Buy Now' then evt_offer_token else evt_consideration_token end nft_token
          ,case when purchase_method = 'Buy Now' then evt_offer_amount else evt_consideration_amount end nft_amount
          ,case when purchase_method = 'Buy Now' then evt_offer_identifier else evt_consideration_identifier end nft_identifier
          ,case when purchase_method = 'Buy Now' then evt_offer_item_type else evt_consideration_item_type end nft_item_type
          ,case when purchase_method = 'Buy Now' then evt_consideration_token else evt_offer_token end price_token
          ,case when purchase_method = 'Buy Now' then evt_consideration_amount + coalesce(evt_fee_amount,0) + coalesce(evt_royalty_amount,0) else evt_offer_amount end price_amount
          ,case when purchase_method = 'Buy Now' then evt_consideration_identifier else evt_offer_identifier end price_identifier
          ,case when purchase_method = 'Buy Now' then evt_consideration_item_type else evt_offer_item_type end price_item_type
          ,e.*
      from p1_evt e
)
,p1_txn_level as (
    select a.main_type
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.zone
          ,a.exchange_contract_address
          ,max(recipient) as buyer
          ,max(offerer) as seller
          ,max(offer_token) as nft_address
          ,max(nft_item_type) as offer_item_type
          ,max(order_type_id) as order_type_id
          ,max(offer_identifier) as nft_token_id
          ,count(evt_tx_hash) as nft_transfer_cnt
          ,max(price_token) as price_token
          ,max(price_item_type) as price_item_type
          ,sum(price_amount) as price_amount
          ,sum(fee_amount) as fee_amount
          ,sum(royalty_amount) as royalty_amount
          ,sum(price_amount) as evt_price_amount
          ,sum(evt_fee_amount) as evt_fee_amount
          ,sum(evt_royalty_amount) as evt_royalty_amount
          ,max(evt_fee_token) as evt_fee_token
          ,max(evt_royalty_token) as evt_royalty_token
          ,max(evt_fee_recipient) as evt_fee_recipient
          ,max(evt_royalty_recipient) as evt_royalty_recipient
          ,count(distinct recipient) as buyer_dcnt
          ,count(distinct offerer) as seller_dcnt
          ,count(distinct offer_token) as nft_address_dcnt
          ,count(distinct nft_item_type) as offer_item_type_dcnt
          ,count(distinct offer_identifier) as nft_token_id_dcnt
          ,count(distinct evt_fee_recipient) as evt_fee_recipient_dcnt
          ,count(distinct evt_royalty_recipient) as evt_royalty_recipient_dcnt
          ,count(1) as attempt_cnt
          ,count(evt_tx_hash) as trade_cnt
          ,count(1) - count(evt_tx_hash) as revert_cnt
          ,coalesce(sum(price_amount),0) as attempt_amount
          ,coalesce(sum(case when evt_tx_hash is not null then coalesce(price_amount,0) end),0) as trade_amount
          ,coalesce(sum(case when evt_tx_hash is null then coalesce(price_amount,0) end),0) as revert_amount
          ,count(case when nft_item_type = '2' then nft_amount end) as erc721_transfer_count 
          ,count(case when nft_item_type = '3' then nft_amount end) as erc1155_transfer_count 
          ,count(case when nft_item_type in ('2','3') then nft_amount end) as nft_transfer_count 
          ,coalesce(sum(case when nft_item_type = '2' then nft_amount end),0) as erc721_item_count 
          ,coalesce(sum(case when nft_item_type = '3' then nft_amount end),0) as erc1155_item_count 
          ,coalesce(sum(case when nft_item_type in ('2','3') then nft_amount end),0) as nft_item_count 
          ,max(purchase_method) as order_type
          ,max(purchase_method) as purchase_method
      from p1_evt_add a
     group by 1,2,3,4,5,6
)
,p1_nft_trades as ( 
    select a.block_time
          ,case when nft_address_dcnt = 1 then concat('\x',substr(a.nft_address,3,40))::bytea end as nft_contract_address 
          ,case when nft_address_dcnt = 1 then n.name end as nft_project_name 
          ,case when nft_token_id_dcnt = 1 then a.nft_token_id  end as nft_token_id
          ,case when erc721_transfer_count > 0 and erc1155_transfer_count = 0 then 'erc721'
                when erc721_transfer_count = 0 and erc1155_transfer_count > 0 then 'erc1155'
                when erc721_transfer_count > 0 and erc1155_transfer_count > 0 then 'mixed'
           end as erc_standard
          ,case when purchase_method = 'Offer Accepted' then 'Individual Offer'
                else 'Buy Now'
           end as order_type
          ,a.purchase_method
          ,case when nft_transfer_count = 1 then 'Single Item Trade'
                when nft_transfer_count > 1 and nft_address_dcnt = 1 and nft_token_id_dcnt = 1 then 'Single Item Trade'
                else 'Bulk Purchase'
           end as trade_type
          ,nft_transfer_count
          ,nft_item_count
          ,case when seller_dcnt = 1 then concat('\x',substr(seller,3,40))::bytea end as seller 
          ,case when buyer_dcnt = 1 then concat('\x',substr(buyer,3,40))::bytea end as buyer
          ,concat('\x',substr(a.price_token,3,40))::bytea as original_currency_contract
          ,case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else concat('\x',substr(a.price_token,3,40))::bytea
           end as currency_contract
          ,case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t1.symbol
           end as original_currency 
          ,a.trade_amount / 10^t1.decimals as price_amount
          ,a.trade_amount as price_amount_raw
          ,a.trade_amount / 10^t1.decimals * p1.price as price_usd_amount
          ,case when evt_fee_recipient_dcnt = 1 then concat('\x',substr(evt_fee_recipient,3,40))::bytea end as fee_receive_address
          ,a.evt_fee_amount / 10^t1.decimals as fee_amount
          ,a.evt_fee_amount as fee_amount_raw
          ,a.evt_fee_amount / 10^t1.decimals * p1.price as fee_usd_amount
          ,case when evt_royalty_recipient_dcnt = 1 then concat('\x',substr(evt_royalty_recipient,3,40))::bytea end as royalty_receive_address
          ,a.evt_royalty_amount / 10^t1.decimals as royalty_amount
          ,a.evt_royalty_amount as royalty_amount_raw
          ,a.evt_royalty_amount / 10^t1.decimals * p1.price as royalty_usd_amount
          ,erc721_transfer_count
          ,erc1155_transfer_count
          ,erc721_item_count
          ,erc1155_item_count
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
          ,main_type as call_function
          ,order_type_id
          ,NULL::text as param1
          ,NULL::text as param2
          ,NULL::text as param3
      from p1_txn_level a
          left join ethereum.transactions tx on tx.hash = a.tx_hash 
                                              and tx.block_number > 14801608
                                              and tx.block_time >= p_start_ts
                                              and tx.block_time < p_end_ts
          left join nft.tokens n on n.contract_address = concat('\x',substr(a.nft_address,3,40))::bytea
          left join erc20.tokens t1 on t1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                  else concat('\x',substr(a.price_token,3,40))::bytea
                                                              end                                
          left join prices.usd p1 on p1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                 else concat('\x',substr(a.price_token,3,40))::bytea
                                                            end
                                  and p1.minute = date_trunc('minute', a.block_time)
                                  and p1.minute >= p_start_ts
                                  and p1.minute < p_end_ts
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
       and call_block_time >= p_start_ts
       and call_block_time < p_end_ts     
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
       and call_block_time >= p_start_ts
       and call_block_time < p_end_ts     
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
           -- if we want to get reverted trades, then do left join
                                                            and e.offerer = concat('\x',substr(c.offerer,3,40))::bytea 
                                                            and e.offer->0->>'token' = c.offer_token
                                                            and e.offer->0->>'identifier' = c.offer_identifier
                                                            and e.offer->0->>'itemType' = c.offer_item_type
)
,p2_txn_level as (
    select a.main_type
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.exchange_contract_address
          ,max(a.zone) as zone
          ,max(recipient) as buyer
          ,max(offerer) as seller
          ,max(offer_token) as nft_address
          ,max(offer_item_type) as offer_item_type
          ,max(offer_order_type) as offer_order_type
          ,max(offer_identifier) as nft_token_id
          ,count(evt_tx_hash) as nft_transfer_cnt
          ,max(price_token) as price_token
          ,max(price_item_type) as price_item_type
          ,sum(price_amount) as price_amount
          ,sum(fee_amount) as fee_amount
          ,sum(royalty_amount) as royalty_amount
          ,sum(evt_price_amount) as evt_price_amount
          ,sum(evt_fee_amount) as evt_fee_amount
          ,sum(evt_royalty_amount) as evt_royalty_amount
          ,max(evt_fee_token) as evt_fee_token
          ,max(evt_royalty_token) as evt_royalty_token
          ,max(evt_fee_recipient) as evt_fee_recipient
          ,max(evt_royalty_recipient) as evt_royalty_recipient
          ,count(distinct recipient) as buyer_dcnt
          ,count(distinct offerer) as seller_dcnt
          ,count(distinct offer_token) as nft_address_dcnt
          ,count(distinct case when offer_item_type in ('2','3') then offer_item_type end) as offer_item_type_dcnt
          ,count(distinct offer_identifier) as nft_token_id_dcnt
          ,count(distinct evt_fee_recipient) as evt_fee_recipient_dcnt
          ,count(distinct evt_royalty_recipient) as evt_royalty_recipient_dcnt
          ,count(1) as attempt_cnt
          ,count(evt_tx_hash) as trade_cnt
          ,count(1) - count(evt_tx_hash) as revert_cnt
          ,coalesce(sum(coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0)),0) as attempt_amount
          ,coalesce(sum(case when evt_tx_hash is not null then coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) end),0) as trade_amount
          ,coalesce(sum(case when evt_tx_hash is null then coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) end),0) as revert_amount
          ,count(case when offer_item_type = '2' then evt_token_amount end) as erc721_transfer_count 
          ,count(case when offer_item_type = '3' then evt_token_amount end) as erc1155_transfer_count 
          ,count(case when offer_item_type in ('2','3') then evt_token_amount end) as nft_transfer_count 
          ,coalesce(sum(case when offer_item_type = '2' then evt_token_amount end),0) as erc721_item_count 
          ,coalesce(sum(case when offer_item_type = '3' then evt_token_amount end),0) as erc1155_item_count 
          ,coalesce(sum(case when offer_item_type in ('2','3') then evt_token_amount end),0) as nft_item_count 
          ,'Bulk Purchase' as trade_type
          ,'Bulk Purchase' as order_type
          ,'Buy Now' as purchase_method
      from p2_evt a
     group by 1,2,3,4,5
)
,p2_nft_trades as ( 
    select a.block_time
          ,case when nft_address_dcnt = 1 then concat('\x',substr(a.nft_address,3,40))::bytea end as nft_contract_address 
          ,case when nft_address_dcnt = 1 then n.name end as nft_project_name 
          ,case when nft_token_id_dcnt = 1 then a.nft_token_id  end as nft_token_id
          ,case when erc721_transfer_count > 0 and erc1155_transfer_count = 0 then 'erc721'
                when erc721_transfer_count = 0 and erc1155_transfer_count > 0 then 'erc1155'
                when erc721_transfer_count > 0 and erc1155_transfer_count > 0 then 'mixed'
           end as erc_standard 
          ,a.order_type
          ,a.purchase_method
          ,case when order_type = 'Bulk Purchase' then 'Bulk Purchase'
                when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_transfer_count 
          ,nft_item_count 
          ,case when seller_dcnt = 1 then concat('\x',substr(seller,3,40))::bytea end as seller 
          ,case when buyer_dcnt = 1 then concat('\x',substr(buyer,3,40))::bytea end as buyer
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
          ,case when evt_fee_recipient_dcnt = 1 then concat('\x',substr(evt_fee_recipient,3,40))::bytea end as fee_receive_address
          ,a.evt_fee_amount / 10^t1.decimals as fee_amount
          ,a.evt_fee_amount as fee_amount_raw
          ,a.evt_fee_amount / 10^t1.decimals * p1.price as fee_usd_amount
          ,case when evt_royalty_recipient_dcnt = 1 then concat('\x',substr(evt_royalty_recipient,3,40))::bytea end as royalty_receive_address
          ,a.evt_royalty_amount / 10^t1.decimals as royalty_amount
          ,a.evt_royalty_amount as royalty_amount_raw
          ,a.evt_royalty_amount / 10^t1.decimals * p1.price as royalty_usd_amount
          ,erc721_transfer_count
          ,erc1155_transfer_count
          ,erc721_item_count
          ,erc1155_item_count
        --   ,attempt_cnt
        --   ,trade_cnt
        --   ,revert_cnt
        --   ,attempt_amount / 10^t1.decimals as attempt_amount
        --   ,trade_amount / 10^t1.decimals as trade_amount
        --   ,revert_amount / 10^t1.decimals as revert_amount
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
          ,main_type as call_function
          ,offer_order_type as order_type_id  -- tobe
          ,NULL::text as param1   -- tobe
          ,NULL::text as param2   -- tobe
          ,NULL::text as param3   -- tobe
      from p2_txn_level a
          left join ethereum.transactions tx on tx.hash = a.tx_hash 
                                              and tx.block_number > 14801608
                                              and tx.block_time >= p_start_ts
                                              and tx.block_time < p_end_ts
          left join nft.tokens n on n.contract_address = concat('\x',substr(a.nft_address,3,40))::bytea
          left join erc20.tokens t1 on t1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                  else concat('\x',substr(a.price_token,3,40))::bytea
                                                              end                                
          left join prices.usd p1 on p1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                 else concat('\x',substr(a.price_token,3,40))::bytea
                                                            end
                                  and p1.minute = date_trunc('minute', a.block_time)
                                  and p1.minute >= p_start_ts
                                  and p1.minute < p_end_ts
)
,p3_call as (
    select 'order' as main_type
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,max("order"->'parameters'->>'orderType') as order_type_id
      from seaport."Seaport_call_fulfillOrder"
     where 1=1
       and call_block_time >= p_start_ts
       and call_block_time < p_end_ts     
     group by 1,2,3,4
     union all
    select 'advanced_order' as main_type
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,max("advancedOrder"->'parameters'->>'orderType') as order_type_id
      from seaport."Seaport_call_fulfillAdvancedOrder"
     where 1=1
       and call_block_time >= p_start_ts
       and call_block_time < p_end_ts     
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
)
,p3_add_rn as (
    select case when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 1 then 'fee'
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
)
,p3_txn_level as (
    select tx_hash
          ,block_time
          ,block_number
          ,zone
          ,exchange_contract_address
          ,0 as evt_index
          ,max(order_type) as order_type
          ,max(purchase_method) as purchase_method
          ,count(distinct case when item_type in ('2','3') then receiver end) as buyer_dcnt
          ,max(case when item_type in ('2','3') then receiver::text end)::bytea as buyer
          ,count(distinct case when item_type in ('2','3') then sender end) as seller_dcnt
          ,max(case when item_type in ('2','3') then sender::text end)::bytea as seller
          ,sum(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 1 then original_amount
                    when purchase_method = 'Buy Now' and sub_type = 'consideration' then original_amount
               end) as original_amount
          ,max(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                    when purchase_method = 'Buy Now' and sub_type = 'consideration' then token_contract_address::text
               end)::bytea as original_currency_contract
          ,max(case when fee_royalty_yn = 'fee' then receiver::text end)::bytea as fee_receive_address
          ,sum(case when fee_royalty_yn = 'fee' then original_amount end) as fee_amount
          ,max(case when fee_royalty_yn = 'fee' then token_contract_address::text end)::bytea as fee_currency_contract
          ,max(case when fee_royalty_yn = 'royalty' then receiver::text end)::bytea as royalty_receive_address
          ,sum(case when fee_royalty_yn = 'royalty' then original_amount end) as royalty_amount
          ,max(case when fee_royalty_yn = 'royalty' then token_contract_address::text end)::bytea as royalty_currency_contract
          ,max(case when item_type in ('2','3') then token_contract_address::text end)::bytea as nft_contract_address
          ,max(case when item_type in ('2','3') then token_id end) as nft_token_id
          ,count(case when item_type = '2' then 1 end) as erc721_transfer_count
          ,count(case when item_type = '3' then 1 end) as erc1155_transfer_count
          ,count(case when item_type in ('2','3') then 1 end) as nft_transfer_count
          ,coalesce(sum(case when item_type = '2' then original_amount end),0) as erc721_item_count
          ,coalesce(sum(case when item_type = '3' then original_amount end),0) as erc1155_item_count
          ,coalesce(sum(case when item_type in ('2','3') then original_amount end),0) as nft_item_count 
          ,max(main_type) as main_type
          ,max(order_type_id) as order_type_id
      from p3_add_rn a
     group by 1,2,3,4,5
)
,p3_nft_trades as (
    select a.block_time
          ,a.nft_contract_address
          ,n.name as nft_project_name
          ,nft_token_id
          ,case when erc721_transfer_count > 0 and erc1155_transfer_count = 0 then 'erc721'
                when erc721_transfer_count = 0 and erc1155_transfer_count > 0 then 'erc1155'
                when erc721_transfer_count > 0 and erc1155_transfer_count > 0 then 'mixed'
           end as erc_standard
          ,order_type
          ,purchase_method
          ,case when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_transfer_count 
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
          ,a.fee_amount / 10^t1.decimals as fee_amount
          ,a.fee_amount as fee_amount_raw
          ,a.fee_amount / 10^t1.decimals * p1.price as fee_usd_amount
          ,a.royalty_receive_address
          ,a.royalty_amount / 10^t1.decimals as royalty_amount
          ,a.royalty_amount as royalty_amount_raw
          ,a.royalty_amount / 10^t1.decimals * p1.price as royalty_usd_amount
          ,erc721_transfer_count
          ,erc1155_transfer_count
          ,erc721_item_count
          ,erc1155_item_count
        --   ,nft_transfer_count as attempt_cnt
        --   ,nft_transfer_count as trade_cnt
        --   ,0 as revert_cnt
        --   ,original_amount / 10^t1.decimals as attempt_amount
        --   ,original_amount / 10^t1.decimals as trade_amount
        --   ,0 as revert_amount
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
          ,main_type as call_function
          ,order_type_id
          ,NULL::text param1
          ,NULL::text param2
          ,NULL::text param3
      from p3_txn_level a
           left join ethereum.transactions tx on tx.hash = a.tx_hash
                                              and tx.block_number > 14801608
                                              and tx.block_time >= p_start_ts
                                              and tx.block_time < p_end_ts
           left join nft.tokens n on n.contract_address = a.nft_contract_address
           left join erc20.tokens t1 on t1.contract_address = case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                   else a.original_currency_contract
                                                              end                                
           left join prices.usd p1 on p1.contract_address = case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                 else a.original_currency_contract
                                                            end
                                   and p1.minute = date_trunc('minute', a.block_time)
                                   and p1.minute >= p_start_ts
                                   and p1.minute < p_end_ts
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
       and call_block_time >= p_start_ts
       and call_block_time < p_end_ts
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
       and call_block_time >= p_start_ts
       and call_block_time < p_end_ts
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
,p4_txn_level as (
    select a.main_type
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.zone
          ,a.exchange_contract_address
          ,max(recipient) as buyer
          ,count(distinct recipient) as buyer_dcnt
          ,max(offerer) as seller
          ,count(distinct offerer) as seller_dcnt
          ,max(offer_token) as nft_address
          ,count(distinct offer_token) as nft_address_dcnt
          ,max(offer_item_type) as offer_item_type
          ,count(distinct case when offer_item_type in ('2','3') then offer_item_type end) as offer_item_type_dcnt
          ,max(offer_identifier) as nft_token_id
          ,count(distinct offer_identifier) as nft_token_id_dcnt
          ,count(tx_hash) as nft_transfer_cnt
          ,max(price_token) as price_token
          ,max(price_item_type) as price_item_type
          ,sum(price_amount) as price_amount
          ,sum(fee_amount) as fee_amount
          ,sum(royalty_amount) as royalty_amount
          ,sum(price_amount) as evt_price_amount
          ,sum(fee_amount) as evt_fee_amount
          ,sum(royalty_amount) as evt_royalty_amount
          ,max(fee_token) as evt_fee_token
          ,max(royalty_token) as evt_royalty_token
          ,count(distinct fee_recipient) as evt_fee_recipient_dcnt
          ,max(fee_recipient) as evt_fee_recipient
          ,count(distinct royalty_recipient) as evt_royalty_recipient_dcnt
          ,max(royalty_recipient) as evt_royalty_recipient
          ,count(1) as attempt_cnt
          ,count(tx_hash) as trade_cnt
          ,count(1) - count(tx_hash) as revert_cnt
          ,coalesce(sum(coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0)),0) as attempt_amount
          ,coalesce(sum(case when tx_hash is not null then coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) end),0) as trade_amount
          ,coalesce(sum(case when tx_hash is null then coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) end),0) as revert_amount
          ,count(case when offer_item_type = '2' then 1 end) as erc721_transfer_count 
          ,count(case when offer_item_type = '3' then 1 end) as erc1155_transfer_count 
          ,count(case when offer_item_type in ('2','3') then 1 end) as nft_transfer_count 
          ,coalesce(sum(case when offer_item_type = '2' then offer_amount end),0) as erc721_item_count 
          ,coalesce(sum(case when offer_item_type = '3' then offer_amount end),0) as erc1155_item_count 
          ,coalesce(sum(case when offer_item_type in ('2','3') then offer_amount end),0) as nft_item_count 
          ,'' as offer_order_type
          ,'Private Sales' as order_type
          ,'Buy Now' as purchase_method
      from p4_add_rn a
     where offer_item_type in ('2','3')
     group by 1,2,3,4,5,6
)
,p4_nft_trades as ( 
    select a.block_time
          ,case when nft_address_dcnt = 1 then concat('\x',substr(a.nft_address,3,40))::bytea end as nft_contract_address 
          ,case when nft_address_dcnt = 1 then n.name end as nft_project_name 
          ,case when nft_token_id_dcnt = 1 then a.nft_token_id  end as nft_token_id
          ,case when erc721_transfer_count > 0 and erc1155_transfer_count = 0 then 'erc721'
                when erc721_transfer_count = 0 and erc1155_transfer_count > 0 then 'erc1155'
                when erc721_transfer_count > 0 and erc1155_transfer_count > 0 then 'mixed'
           end as erc_standard
          ,a.order_type
          ,a.purchase_method
          ,case when order_type = 'Bulk Purchase' then 'Bulk Purchase'
                when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_transfer_count 
          ,nft_item_count
          ,case when seller_dcnt = 1 then concat('\x',substr(seller,3,40))::bytea end as seller 
          ,case when buyer_dcnt = 1 then concat('\x',substr(buyer,3,40))::bytea end as buyer
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
          ,case when evt_fee_recipient_dcnt = 1 then concat('\x',substr(evt_fee_recipient,3,40))::bytea end as fee_receive_address
          ,a.evt_fee_amount / 10^t1.decimals as fee_amount
          ,a.evt_fee_amount as fee_amount_raw
          ,a.evt_fee_amount / 10^t1.decimals * p1.price as fee_usd_amount
          ,case when evt_royalty_recipient_dcnt = 1 then concat('\x',substr(evt_royalty_recipient,3,40))::bytea end as royalty_receive_address
          ,a.evt_royalty_amount / 10^t1.decimals as royalty_amount
          ,a.evt_royalty_amount as royalty_amount_raw
          ,a.evt_royalty_amount / 10^t1.decimals * p1.price as royalty_usd_amount
          ,erc721_transfer_count
          ,erc1155_transfer_count
          ,erc721_item_count
          ,erc1155_item_count
        --   ,attempt_cnt
        --   ,trade_cnt
        --   ,revert_cnt
        --   ,attempt_amount / 10^t1.decimals as attempt_amount
        --   ,trade_amount / 10^t1.decimals as trade_amount
        --   ,revert_amount / 10^t1.decimals as revert_amount
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
          ,main_type as call_function
          ,offer_order_type as order_type_id 
          ,NULL::text as param1
          ,NULL::text as param2
          ,NULL::text as param3
      from p4_txn_level a
          left join ethereum.transactions tx on tx.hash = a.tx_hash 
                                              and tx.block_number > 14801608
                                              and tx.block_time >= p_start_ts
                                              and tx.block_time < p_end_ts
          left join nft.tokens n on n.contract_address = concat('\x',substr(a.nft_address,3,40))::bytea
          left join erc20.tokens t1 on t1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                  else concat('\x',substr(a.price_token,3,40))::bytea
                                                              end                                
          left join prices.usd p1 on p1.contract_address = case when concat('\x',substr(a.price_token,3,40))::bytea = '\x0000000000000000000000000000000000000000'::bytea then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                                                 else concat('\x',substr(a.price_token,3,40))::bytea
                                                            end
                                  and p1.minute = date_trunc('minute', a.block_time)
                                  and p1.minute >= p_start_ts
                                  and p1.minute < p_end_ts
)
,rows AS (
    INSERT INTO seaport.transactions
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
    ON CONFLICT (tx_hash) DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$
;

-- backfill
SELECT seaport.insert_transactions('2022-06-11', '2022-06-16');
SELECT seaport.insert_transactions('2022-06-16', '2022-06-21');
SELECT seaport.insert_transactions('2022-06-21', '2022-06-26');
SELECT seaport.insert_transactions('2022-06-26', '2022-07-01');
SELECT seaport.insert_transactions('2022-07-01', (SELECT current_timestamp - interval '20 minutes'));

-- cronjob
INSERT INTO cron.job (schedule, command)
VALUES ('*/20 * * * *', 
$$SELECT seaport.insert_transactions((SELECT date_trunc('day',MAX(block_time)) FROM seaport.transactions)
                                            ,(SELECT current_timestamp - interval '20 minutes'));$$
       )
ON CONFLICT (command) 
DO UPDATE SET schedule=EXCLUDED.schedule;