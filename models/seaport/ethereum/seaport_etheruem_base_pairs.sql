{{ config(
    alias = 'base_pairs',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "seaport",
                            \'["sohawk"]\') }}'
    )
}}

with iv_base_pair as (
    select evt_block_time as block_time
          ,evt_block_number as block_number
          ,evt_tx_hash as tx_hash
          ,evt_index
          ,'offer' as sub_type
          ,offer_idx as sub_idx
          ,case offer->0->>'itemType' 
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
           end as offer_first_item_type
          ,case consideration->0->>'itemType'
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' 
           end as consideration_first_item_type          
          ,offerer as sender
          ,recipient as receiver
          ,zone
          ,concat('\x',substr(offer_item->>'token',3,40))::bytea as token_contract_address 
          ,(offer_item->>'amount')::numeric as original_amount
          ,case offer_item->>'itemType'
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' 
           end as item_type
          ,(offer_item->>'identifier') as token_id
          ,contract_address as exchange_contract_address
          ,jsonb_array_length(offer) as offer_cnt
          ,jsonb_array_length(consideration) as consideration_cnt
          ,case when recipient = '\x0000000000000000000000000000000000000000'::bytea then true
                else false
           end as is_private
      from seaport."Seaport_evt_OrderFulfilled" a
          ,jsonb_array_elements(offer) with ordinality as t (offer_item, offer_idx)
    union all
    select evt_block_time as block_time
          ,evt_block_number as block_number
          ,evt_tx_hash as tx_hash
          ,evt_index
          ,'consideration' as sub_type
          ,consideration_idx as sub_idx
          ,case offer->0->>'itemType' 
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
           end as offer_first_item_type
          ,case consideration->0->>'itemType'
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' 
           end as consideration_first_item_type          
          ,recipient as sender
          ,concat('\x',substr(consideration_item->>'recipient',3,40))::bytea as receiver
          ,zone
          ,concat('\x',substr(consideration_item->>'token',3,40))::bytea as token_contract_address
          ,(consideration_item->>'amount')::numeric as original_amount
          ,case consideration_item->>'itemType'
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' -- actually not exists
           end as item_type
          ,(consideration_item->>'identifier') as token_id
          ,contract_address as exchange_contract_address
          ,jsonb_array_length(offer) as offer_cnt
          ,jsonb_array_length(consideration) as consideration_cnt
          ,case when recipient = '\x0000000000000000000000000000000000000000'::bytea then true
                else false
           end as is_private
      from seaport."Seaport_evt_OrderFulfilled" a
          ,jsonb_array_elements(consideration) with ordinality as t (consideration_item, consideration_idx)
)       
,iv_base_pair_add as (
    select a.*
          ,case when offer_first_item_type = 'erc20' then 'offer accepted'
                when offer_first_item_type in ('erc721','erc1155') then 'buy'
                else 'etc' -- some txns has no nfts
           end as order_type
          ,case when offer_first_item_type = 'erc20' and sub_type = 'offer' and item_type = 'erc20' then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and item_type in ('native','erc20') then true
                else false
           end is_price
          ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx = 0 then true  -- offer accepted has no price at all. it has to be calculated.
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx = 1 then true
                else false
           end is_netprice
          ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx = 1 then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx = 2 then true
                else false
           end is_platform_fee
          ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx > 1 then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx > 2 then true
                else false
           end is_creator_fee
          ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx > 1 then eth_erc_idx - 1
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx > 2 then eth_erc_idx - 2
           end creator_fee_idx
          ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721','erc1155') then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721','erc1155') then true
                else false
           end is_traded_nft
          ,case when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and item_type in ('erc721','erc1155') then true
                else false
           end is_moved_nft
      from (select 
                   a.*
                  ,case when item_type in ('native','erc20') then sum(case when item_type in ('native','erc20') then 1 end) over (partition by tx_hash, evt_index, sub_type order by sub_idx) end as eth_erc_idx
                  ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721','erc1155') then 1
                            when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721','erc1155') then 1
                       end) over (partition by tx_hash, evt_index) as nft_cnt
                  ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721') then 1
                            when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721') then 1
                       end) over (partition by tx_hash, evt_index) as erc721_cnt
                  ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc1155') then 1
                            when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc1155') then 1
                       end) over (partition by tx_hash, evt_index) as erc1155_cnt
              from iv_base_pair a
           ) a
)
select *
  from iv_base_pair_add
;
