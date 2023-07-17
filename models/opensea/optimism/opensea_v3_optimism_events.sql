{{ config(
    schema = 'opensea_v3_optimism',
    alias = alias('events'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                            "project",
                            "opensea",
                            \'["sohwak"]\') }}'
)
}}

-- project/version : opensea/v3
-- contract_address : 0x00000000006c3852cbef3e08e8df289169ede581 (Seaport v1.1)

{% set c_native_token_address = "0x0000000000000000000000000000000000000000" %}
{% set c_alternative_token_address = "0x4200000000000000000000000000000000000006" %}  -- WETH
{% set c_native_symbol = "ETH" %}
{% set c_seaport_first_date = "2022-07-01" %}

with source_optimism_transactions as (
    select *
      from {{ source('optimism','transactions') }}
    {% if not is_incremental() %}
     where block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
     where block_time >= date_trunc("day", now() - interval '3 week')
    {% endif %}
)
,ref_tokens_nft as (
    select *
      from {{ ref('tokens_nft') }}
     where blockchain = 'optimism'
)
,ref_tokens_erc20 as (
    select *
      from {{ ref('tokens_erc20') }}
    where blockchain = 'optimism'
)
,ref_nft_aggregators as (
    select *
      from {{ ref('nft_aggregators') }}
    where blockchain = 'optimism'
)
,source_prices_usd as (
    select *
      from {{ source('prices', 'usd') }}
    where blockchain = 'optimism'
    {% if not is_incremental() %}
       and minute >= date '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
       and minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,iv_platform_fee_wallet (wallet_address, wallet_name) as (
    values   ('0x0000a26b00c1f0df003000390027140000faa719','opensea')
)
,iv_offer_consideration as (
      select evt_block_time as block_time
            ,evt_block_number as block_number
            ,evt_tx_hash as tx_hash
            ,evt_index
            ,'offer' as sub_type
            ,offer_idx + 1 as sub_idx
            ,case offer[0]:itemType 
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end as offer_first_item_type
            ,case consideration[0]:itemType
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' 
            end as consideration_first_item_type
            ,offerer
            ,recipient
            ,offerer as sender
            ,recipient as receiver
            ,zone
            ,offer_item:token as token_contract_address 
            ,cast(offer_item:amount as numeric(38)) as original_amount
            ,case offer_item:itemType
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' 
            end as item_type
            ,offer_item:identifier as token_id
            ,contract_address as platform_contract_address
            ,size(offer) as offer_cnt
            ,size(consideration) as consideration_cnt
            ,order_hash
            ,false as is_private -- will be deprecated in base_pairs
    from
    (
        select consideration
            , contract_address
            , evt_block_number
            , evt_block_time
            , evt_index
            , evt_tx_hash
            , offer
            , offerer
            , recipient
            , zone
            , orderHash AS order_hash
            , posexplode(offer) as (offer_idx, offer_item)
        from {{ source('opensea_optimism', 'Seaport_evt_OrderFulfilled') }}
       where contract_address = '0x00000000006c3852cbef3e08e8df289169ede581'
         and recipient != '0x0000000000000000000000000000000000000000'
       {% if not is_incremental() %}
         and evt_block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
       {% endif %}
       {% if is_incremental() %}
         and evt_block_time >= date_trunc("day", now() - interval '1 week')
       {% endif %}
    )
    union all
    select evt_block_time as block_time
            ,evt_block_number as block_number
            ,evt_tx_hash as tx_hash
            ,evt_index
            ,'consideration' as sub_type
            ,consideration_idx + 1 as sub_idx
            ,case offer[0]:itemType 
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end as offer_first_item_type
            ,case consideration[0]:itemType
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' 
            end as consideration_first_item_type          
            ,offerer
            ,recipient
            ,recipient as sender
            ,consideration_item:recipient as receiver
            ,zone
            ,consideration_item:token as token_contract_address
            ,cast(consideration_item:amount as numeric(38)) as original_amount
            ,case consideration_item:itemType
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' -- actually not exists
            end as item_type
            ,consideration_item:identifier as token_id
            ,contract_address as platform_contract_address
            ,size(offer) as offer_cnt
            ,size(consideration) as consideration_cnt
            ,order_hash
            ,false as is_private -- will be deprecated in base_pairs
    from
    (
        select consideration
              ,contract_address
              ,evt_block_number
              ,evt_block_time
              ,evt_index
              ,evt_tx_hash
              ,offer
              ,offerer
              ,recipient
              ,zone
              ,orderHash AS order_hash
              ,posexplode(consideration) as (consideration_idx, consideration_item)
          from {{ source('opensea_optimism','Seaport_evt_OrderFulfilled') }}
         where contract_address = '0x00000000006c3852cbef3e08e8df289169ede581'
           and recipient != '0x0000000000000000000000000000000000000000'
        {% if not is_incremental() %}
           and evt_block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
        {% endif %}
        {% if is_incremental() %}
           and evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    )
)
,iv_match_output as (
    select block_time
          ,block_number
          ,tx_hash
          ,evt_index
          ,sub_type
          ,sub_idx 
          ,case offer_first_item
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                when '4' then 'erc721'
                when '5' then 'erc1155'
                else 'etc'
           end as offer_first_item_type
          ,case consider_first_item
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                when '4' then 'erc721'
                when '5' then 'erc1155'
                else 'etc'
           end as consideration_first_item_type
          ,offerer
          ,receiver as recipient
          ,sender
          ,receiver
          ,zone
          ,token_contract_address
          ,cast(original_amount as numeric(38)) as original_amount
          ,case item_type_code 
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                when '4' then 'erc721'
                when '5' then 'erc1155'
                else 'etc'
           end as item_type
          ,token_id
          ,platform_contract_address
          ,1 as offer_cnt
          ,1 as consideration_cnt
          ,NULL as order_hash
          ,false as is_private
      from (select call_block_time as block_time
                  ,call_block_number as block_number
                  ,call_tx_hash as tx_hash
                  ,dense_rank() over (partition by call_tx_hash order by call_trace_address) as evt_index
                  ,'match_adv_ord' as sub_type
                  ,execution_idx + 1 as sub_idx
                  ,advancedOrders[0]:parameters:zone as zone
                  ,advancedOrders[0]:parameters:offerer as offerer
                  ,advancedOrders[0]:parameters:offer[0]:itemType as offer_first_item
                  ,advancedOrders[0]:parameters:consideration[0]:itemType as consider_first_item
                  ,execution:offerer as sender
                  ,execution:item:token as token_contract_address
                  ,execution:item:amount as original_amount
                  ,execution:item:itemType as item_type_code
                  ,execution:item:identifier as token_id
                  ,execution:item:recipient as receiver
                  ,contract_address as platform_contract_address
            from (select *
                        ,posexplode(output_executions) as (execution_idx, execution) 
                    from {{ source('opensea_optimism', 'Seaport_call_matchAdvancedOrders') }}
                   where call_success 
                     and contract_address = '0x00000000006c3852cbef3e08e8df289169ede581'  -- Seaport v1.1
                 {% if not is_incremental() %}
                     and call_block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
                 {% endif %}
                 {% if is_incremental() %}
                     and call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                ) 
            union all
            select call_block_time as block_time
                  ,call_block_number as block_number
                  ,call_tx_hash as tx_hash
                  ,dense_rank() over (partition by call_tx_hash order by call_trace_address) as evt_index
                  ,'match_ord' as sub_type
                  ,execution_idx + 1 as sub_idx
                  ,orders[0]:parameters:zone as zone    -- TODO : column would be changed
                  ,orders[0]:parameters:offerer as offerer
                  ,orders[0]:parameters:offer[0]:itemType as offer_first_item
                  ,orders[0]:parameters:consideration[0]:itemType as consider_first_item
                  ,execution:offerer as sender
                  ,execution:item:token as token_contract_address
                  ,execution:item:amount as original_amount
                  ,execution:item:itemType as item_type_code
                  ,execution:item:identifier as token_id
                  ,execution:item:recipient as receiver
                  ,contract_address as platform_contract_address
            from (select *
                        ,posexplode(output_executions) as (execution_idx, execution)   -- output_executions
                    from {{ source('opensea_optimism', 'Seaport_call_matchOrders') }}
                   where call_success 
                     and contract_address = '0x00000000006c3852cbef3e08e8df289169ede581'  -- Seaport v1.1
                 {% if not is_incremental() %}
                     and call_block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
                 {% endif %}
                 {% if is_incremental() %}
                     and call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                ) 


    )
)
,iv_base_pairs as (
    -- fulfill orders
    select a.block_time
            ,a.block_number
            ,a.tx_hash
            ,a.evt_index
            ,a.sub_type
            ,a.sub_idx
            ,a.offer_first_item_type
            ,a.consideration_first_item_type
            ,a.offerer
            ,a.recipient
            ,a.sender
            ,a.receiver
            ,a.zone
            ,a.token_contract_address
            ,a.original_amount
            ,a.item_type
            ,a.token_id
            ,a.platform_contract_address
            ,a.offer_cnt
            ,a.consideration_cnt
            ,a.order_hash
            ,a.is_private
            ,a.is_self_trans
            ,a.is_platform_fee
            ,a.eth_erc_idx
            ,a.nft_cnt
            ,a.erc721_cnt
            ,a.erc1155_cnt
            ,try_cast(date_trunc('day', a.block_time) as date) as block_date
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
            ,case when is_platform_fee then false
                  when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx > 0 then true
                  when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx > 1 then true
                  else false
             end is_creator_fee
            ,sum(case when is_platform_fee then null
                      when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx > 0 then 1
                      when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx > 1 then 1
                 end) over (partition by tx_hash, evt_index order by eth_erc_idx) as creator_fee_idx
            ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721','erc1155') then true
                  when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721','erc1155') then true
                  else false
             end is_traded_nft
            ,case when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and item_type in ('erc721','erc1155') then true
                  else false
             end is_moved_nft
            ,fee_wallet_name
    from (select a.block_time
                ,a.block_number
                ,a.tx_hash
                ,a.evt_index
                ,a.sub_type
                ,a.sub_idx
                ,a.offer_first_item_type
                ,a.consideration_first_item_type
                ,a.offerer
                ,a.recipient
                ,a.sender
                ,a.receiver
                ,a.zone
                ,a.token_contract_address
                ,a.original_amount
                ,a.item_type
                ,a.token_id
                ,a.platform_contract_address
                ,a.offer_cnt
                ,a.consideration_cnt
                ,a.order_hash
                ,a.is_private
                ,f.wallet_name as fee_wallet_name
                ,case when sender = receiver then true else false end is_self_trans
                ,case when f.wallet_address is not null then true else false end as is_platform_fee
                ,case when item_type in ('native','erc20')
                    then sum(case when item_type in ('native','erc20') then 1 end) over (partition by tx_hash, evt_index, sub_type order by sub_idx)
                end as eth_erc_idx
              ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721','erc1155') then 1
                        when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721','erc1155') then 1
                  end) over (partition by tx_hash, evt_index) as nft_cnt
              ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721') then 1
                        when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721') then 1
                  end) over (partition by tx_hash, evt_index) as erc721_cnt
              ,sum(case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc1155') then 1
                        when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc1155') then 1
                  end) over (partition by tx_hash, evt_index) as erc1155_cnt
          from iv_offer_consideration a
              left join iv_platform_fee_wallet f on f.wallet_address = a.receiver
          ) a
    where not is_self_trans
    -- match orders
    union all
    select a.block_time
            ,a.block_number
            ,a.tx_hash
            ,a.evt_index
            ,a.sub_type
            ,a.sub_idx
            ,a.offer_first_item_type
            ,a.consideration_first_item_type
            ,a.offerer
            ,a.recipient
            ,a.sender
            ,a.receiver
            ,a.zone
            ,a.token_contract_address
            ,a.original_amount
            ,a.item_type
            ,a.token_id
            ,a.platform_contract_address
            ,a.offer_cnt
            ,a.consideration_cnt
            ,a.order_hash
            ,a.is_private
            ,a.is_self_trans
            ,a.is_platform_fee
            ,a.eth_erc_idx
            ,a.nft_cnt
            ,a.erc721_cnt
            ,a.erc1155_cnt
            ,try_cast(date_trunc('day', a.block_time) as date) as block_date
            ,case when offer_first_item_type in ('erc20','native') then 'offer accepted'
                  when offer_first_item_type in ('erc721','erc1155') then 'buy'
                  else 'etc' -- some txns has no nfts
             end as order_type
            ,case when item_type in ('native','erc20') then true
                  else false
             end is_price
            ,false as is_netprice -- it has to be calculated.
            ,case when is_platform_fee then false -- to os fee wallet has to be excluded
                  when offer_first_item_type in ('erc721','erc1155') and eth_erc_idx = 1 then false  -- buy = first transfer is is profit
                  when offer_first_item_type in ('erc20','native') and eth_erc_idx = eth_erc_cnt then false  -- offer_accepted = last transfer is is profit
                  when eth_erc_idx > 0 then true  -- else is all creator fees
                  else false
             end is_creator_fee
            ,sum(case when is_platform_fee then null -- to os fee wallet has to be excluded
                      when offer_first_item_type in ('erc721','erc1155') and eth_erc_idx = 1 then null  -- buy = first transfer is is profit
                      when offer_first_item_type in ('erc20','native') and eth_erc_idx = eth_erc_cnt then null  -- offer_accepted = last transfer is is profit
                      when eth_erc_idx > 0 then 1  -- else is all creator fees
                 end) over (partition by tx_hash, evt_index order by eth_erc_idx) as creator_fee_idx
            ,case when item_type in ('erc721','erc1155') then true
                  else false
             end is_traded_nft
            ,false as is_moved_nft
            ,fee_wallet_name
    from  (select a.block_time
                ,a.block_number
                ,a.tx_hash
                ,a.evt_index
                ,a.sub_type
                ,a.sub_idx
                ,a.offer_first_item_type
                ,a.consideration_first_item_type
                ,a.offerer
                ,a.recipient
                ,a.sender
                ,a.receiver
                ,a.zone
                ,a.token_contract_address
                ,a.original_amount
                ,a.item_type
                ,a.token_id
                ,a.platform_contract_address
                ,a.offer_cnt
                ,a.consideration_cnt
                ,a.order_hash
                ,a.is_private
                ,f.wallet_name as fee_wallet_name
                ,case when sender = receiver then true else false end is_self_trans
                ,case when f.wallet_address is not null then true else false end as is_platform_fee
                ,case when item_type in ('native','erc20')
                    then sum(case when item_type in ('native','erc20') then 1 end) over (partition by tx_hash, evt_index, sub_type order by sub_idx)
                end as eth_erc_idx
                ,sum(case when item_type in ('erc721','erc1155') then 1 end) over (partition by tx_hash, evt_index) as nft_cnt
                ,sum(case when item_type in ('erc721') then 1 end) over (partition by tx_hash, evt_index) as erc721_cnt
                ,sum(case when item_type in ('erc1155') then 1 end) over (partition by tx_hash, evt_index) as erc1155_cnt
                ,sum(case when item_type in ('native','erc20') then 1 end) over (partition by tx_hash, evt_index) as eth_erc_cnt
          from iv_match_output a
               left join iv_platform_fee_wallet f on f.wallet_address = a.receiver
          ) a
    where not is_self_trans
)
,iv_volume as (
  select block_date
        ,block_time
        ,tx_hash
        ,evt_index
        ,max(token_contract_address) as token_contract_address
        ,CAST(sum(case when is_price then original_amount end) AS DECIMAL(38,0)) as price_amount_raw
        ,sum(case when is_platform_fee then original_amount end) as platform_fee_amount_raw
        ,max(case when is_platform_fee then receiver end) as platform_fee_receiver
        ,sum(case when is_creator_fee then original_amount end) as creator_fee_amount_raw
        ,sum(case when is_creator_fee and creator_fee_idx = 1 then original_amount end) as creator_fee_amount_raw_1
        ,sum(case when is_creator_fee and creator_fee_idx = 2 then original_amount end) as creator_fee_amount_raw_2
        ,sum(case when is_creator_fee and creator_fee_idx = 3 then original_amount end) as creator_fee_amount_raw_3
        ,sum(case when is_creator_fee and creator_fee_idx = 4 then original_amount end) as creator_fee_amount_raw_4
        ,sum(case when is_creator_fee and creator_fee_idx = 5 then original_amount end) as creator_fee_amount_raw_5
        ,max(case when is_creator_fee and creator_fee_idx = 1 then receiver end) as creator_fee_receiver_1
        ,max(case when is_creator_fee and creator_fee_idx = 2 then receiver end) as creator_fee_receiver_2
        ,max(case when is_creator_fee and creator_fee_idx = 3 then receiver end) as creator_fee_receiver_3
        ,max(case when is_creator_fee and creator_fee_idx = 4 then receiver end) as creator_fee_receiver_4
        ,max(case when is_creator_fee and creator_fee_idx = 5 then receiver end) as creator_fee_receiver_5
          ,max(a.fee_wallet_name) as fee_wallet_name
   from iv_base_pairs a
  where 1=1
    and eth_erc_idx > 0
  group by 1,2,3,4
  having count(distinct token_contract_address) = 1  -- some private sale trade has more that one currencies
)
,iv_nfts as (
  select a.block_date
        ,a.block_time
        ,a.tx_hash
        ,a.evt_index
        ,a.block_number
        ,a.sender as seller
        ,a.receiver as buyer
        ,case when nft_cnt > 1 then 'bundle trade'
              else 'single item trade'
          end as trade_type
        ,a.order_type
        ,a.token_contract_address as nft_contract_address
        ,a.original_amount as nft_token_amount
        ,a.token_id as nft_token_id
        ,a.item_type as nft_token_standard
        ,a.zone
        ,a.platform_contract_address
        ,b.token_contract_address
        ,CAST(round(price_amount_raw / nft_cnt) AS DECIMAL(38,0)) as price_amount_raw  -- to truncate the odd number of decimal places
        ,round(platform_fee_amount_raw / nft_cnt) as platform_fee_amount_raw
        ,platform_fee_receiver
        ,round(creator_fee_amount_raw / nft_cnt) as creator_fee_amount_raw
        ,creator_fee_amount_raw_1 / nft_cnt as creator_fee_amount_raw_1
        ,creator_fee_amount_raw_2 / nft_cnt as creator_fee_amount_raw_2
        ,creator_fee_amount_raw_3 / nft_cnt as creator_fee_amount_raw_3
        ,creator_fee_amount_raw_4 / nft_cnt as creator_fee_amount_raw_4
        ,creator_fee_amount_raw_5 / nft_cnt as creator_fee_amount_raw_5
        ,creator_fee_receiver_1
        ,creator_fee_receiver_2
        ,creator_fee_receiver_3
        ,creator_fee_receiver_4
        ,creator_fee_receiver_5
        ,case when nft_cnt > 1 then true
              else false
          end as estimated_price
        ,is_private
        ,sub_type
        ,sub_idx
        ,order_hash
          ,b.fee_wallet_name
  from iv_base_pairs a
  left join iv_volume b on b.block_date = a.block_date  -- tx_hash and evt_index is PK, but for performance, block_time is included
    and b.tx_hash = a.tx_hash
    and b.evt_index = a.evt_index
  where 1=1
    and a.is_traded_nft
)
,iv_trades as (
    select a.block_date
          ,a.block_time
          ,a.tx_hash
          ,a.evt_index
          ,a.block_number
          ,a.seller
          ,a.buyer
          ,a.trade_type
          ,a.order_type
          ,a.nft_contract_address
          ,a.nft_token_amount
          ,a.nft_token_id
          ,a.nft_token_standard
          ,a.zone
          ,a.platform_contract_address
          ,a.token_contract_address
          ,a.price_amount_raw
          ,a.platform_fee_amount_raw
          ,a.platform_fee_receiver
          ,a.creator_fee_amount_raw
          ,a.creator_fee_amount_raw_1
          ,a.creator_fee_amount_raw_2
          ,a.creator_fee_amount_raw_3
          ,a.creator_fee_amount_raw_4
          ,a.creator_fee_amount_raw_5
          ,a.creator_fee_receiver_1
          ,a.creator_fee_receiver_2
          ,a.creator_fee_receiver_3
          ,a.creator_fee_receiver_4
          ,a.creator_fee_receiver_5
          ,a.estimated_price
          ,a.is_private
          ,a.sub_type
          ,a.sub_idx
          ,n.name AS nft_token_name
          ,t.from as tx_from
          ,t.to as tx_to
          ,right(t.data,8) as right_hash
          ,case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_native_symbol}}'
                else e.symbol
           end as token_symbol
          ,case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_alternative_token_address}}'
                else a.token_contract_address
           end as token_alternative_symbol
          ,e.decimals as price_token_decimals
          ,a.price_amount_raw / power(10, e.decimals) as price_amount
          ,a.price_amount_raw / power(10, e.decimals) * p.price as price_amount_usd
          ,a.platform_fee_amount_raw / power(10, e.decimals) as platform_fee_amount
          ,a.platform_fee_amount_raw / power(10, e.decimals) * p.price as platform_fee_amount_usd
          ,a.creator_fee_amount_raw / power(10, e.decimals) as creator_fee_amount
          ,a.creator_fee_amount_raw / power(10, e.decimals) * p.price as creator_fee_amount_usd
          ,agg.name as aggregator_name
          ,agg.contract_address AS aggregator_address
          ,sub_idx
          ,a.fee_wallet_name
  from iv_nfts a
  inner join source_optimism_transactions t on t.hash = a.tx_hash
  left join ref_tokens_nft n on n.contract_address = nft_contract_address
  left join ref_tokens_erc20 e on e.contract_address = case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_alternative_token_address}}'
                                                            else a.token_contract_address
                                                      end
  left join source_prices_usd p on p.contract_address = case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_alternative_token_address}}'
                                                            else a.token_contract_address
                                                        end
    and p.minute = date_trunc('minute', a.block_time)
  left join ref_nft_aggregators agg on agg.contract_address = t.to
)
select  -- basic info
        'optimism' as blockchain
        ,'opensea' as project
        ,'v3' as version

        -- order info
        ,block_date
        ,block_time
        ,seller
        ,buyer
        ,initcap(trade_type) as trade_type
        ,initcap(order_type) as trade_category -- Buy / Offer Accepted
        ,'Trade' as evt_type

        -- nft token info
        ,nft_contract_address
        ,nft_token_name as collection
        ,nft_token_id as token_id
        ,nft_token_amount as number_of_items
        ,nft_token_standard as token_standard

        -- price info
        ,price_amount as amount_original
        ,price_amount_raw as amount_raw
        ,price_amount_usd as amount_usd
        ,token_symbol as currency_symbol
        ,token_alternative_symbol as currency_contract
        ,token_contract_address as original_currency_contract
        ,price_token_decimals as currency_decimals   -- in case calculating royalty1~4

        -- project info (platform or exchange)
        ,platform_contract_address as project_contract_address
        ,platform_fee_receiver as platform_fee_receive_address
        ,platform_fee_amount_raw
        ,platform_fee_amount
        ,platform_fee_amount_usd
        ,case when price_amount_raw > 0 then CAST ((platform_fee_amount_raw / price_amount_raw * 100) AS DOUBLE) end platform_fee_percentage

        -- royalty info
        ,creator_fee_receiver_1 as royalty_fee_receive_address
        ,creator_fee_amount_raw as royalty_fee_amount_raw
        ,creator_fee_amount as royalty_fee_amount
        ,creator_fee_amount_usd as royalty_fee_amount_usd
        ,case when price_amount_raw > 0 then CAST((creator_fee_amount_raw / price_amount_raw * 100) AS DOUBLE) end royalty_fee_percentage
        ,creator_fee_receiver_1 as royalty_fee_receive_address_1
        ,creator_fee_receiver_2 as royalty_fee_receive_address_2
        ,creator_fee_receiver_3 as royalty_fee_receive_address_3
        ,creator_fee_receiver_4 as royalty_fee_receive_address_4
        ,creator_fee_receiver_5 as royalty_fee_receive_address_5
        ,creator_fee_amount_raw_1 as royalty_fee_amount_raw_1
        ,creator_fee_amount_raw_2 as royalty_fee_amount_raw_2
        ,creator_fee_amount_raw_3 as royalty_fee_amount_raw_3
        ,creator_fee_amount_raw_4 as royalty_fee_amount_raw_4
        ,creator_fee_amount_raw_5 as royalty_fee_amount_raw_5

        -- aggregator
        ,aggregator_name
        ,aggregator_address

        -- tx
        ,block_number
        ,tx_hash
        ,evt_index
        ,tx_from
        ,tx_to
        ,right_hash

        -- seaport etc
        ,zone as zone_address
        ,estimated_price
        ,is_private
        ,sub_idx
        ,sub_type
        ,fee_wallet_name
        ,'seaport-' || CAST(tx_hash AS VARCHAR(100)) || '-' || cast(evt_index as VARCHAR(10)) || '-' || CAST(nft_contract_address AS VARCHAR(100)) || '-' || cast(nft_token_id as VARCHAR(100)) || '-' || cast(sub_type as VARCHAR(20)) || '-' || cast(sub_idx as VARCHAR(10)) as unique_trade_id
  from  iv_trades
 where  CAST(zone AS VARCHAR(100)) in ('0x0000000000000000000000000000000000000000'
                                      ,'0xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                                      ,'0x9b814233894cd227f561b78cc65891aa55c62ad2'
                                      ,'0x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                                      ,'0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd'
                                      ,'0x000000e7ec00e7b300774b00001314b8610022b8' -- newly added on seaport v1.4
                                      )
   and  (    fee_wallet_name = 'opensea'
          or right_hash = '360c6ebe' -- opensea hash
        )
