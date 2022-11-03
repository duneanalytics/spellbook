{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index','nft_contract_address','token_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "seaport",
                            \'["sohawk"]\') }}'
    )
}}

{% set c_native_token_address = "0x0000000000000000000000000000000000000000" %}
{% set c_alternative_token_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" %}
{% set c_native_symbol = "ETH" %}
{% set c_seaport_first_date = "2022-06-01" %}

with source_ethereum_transactions as (
    select *
    from {{ source('ethereum','transactions') }}
    {% if not is_incremental() %}
    where block_time >= date '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,seaport_ethereum_base_pairs as (
  with iv_offer_consideration as (
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
          ,offerer as sender
          ,recipient as receiver
          ,zone
          ,offer_item:token as token_contract_address 
          ,offer_item:amount::numeric(38) as original_amount
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
          ,case when recipient = '0x0000000000000000000000000000000000000000' then true
                else false
          end as is_private
    from
    (
      select *
            ,posexplode(offer) as (offer_idx, offer_item)
      from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }}
      {% if is_incremental() %}
      where evt_block_time >= date_trunc("day", now() - interval '1 week')
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
          ,recipient as sender
          ,consideration_item:recipient as receiver
          ,zone
          ,consideration_item:token as token_contract_address
          ,consideration_item:amount::numeric(38) as original_amount
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
          ,case when recipient = '0x0000000000000000000000000000000000000000' then true
                else false
          end as is_private
    from
    (
      select *
            ,posexplode(consideration) as (consideration_idx, consideration_item)
      from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }}
      {% if is_incremental() %}
      where evt_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
    )
  )
  ,iv_base_pairs as (
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
    from
    (
      select a.*
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
      from iv_offer_consideration a
    ) a
  )
  select *
  from iv_base_pairs
)
,ref_tokens_nft as (
    select *
    from {{ ref('tokens_nft') }}
    where blockchain = 'ethereum'
)
,ref_tokens_erc20 as (
    select *
    from {{ ref('tokens_erc20') }}
    where blockchain = 'ethereum'
)
,ref_nft_aggregators as (
    select *
    from {{ ref('nft_ethereum_aggregators') }}
)
,source_prices_usd as (
    select *
    from {{ source('prices', 'usd') }}
    where blockchain = 'ethereum'
    {% if not is_incremental() %}
      and minute >= date '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %} 
      and minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,iv_base_pairs_priv as (
  select a.block_time
        ,a.block_number
        ,a.tx_hash
        ,a.evt_index
        ,a.sub_type
        ,a.sub_idx
        ,a.offer_first_item_type
        ,a.consideration_first_item_type
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
        ,a.is_private
        ,a.eth_erc_idx
        ,a.nft_cnt
        ,a.erc721_cnt
        ,a.erc1155_cnt
        ,a.order_type
        ,a.is_price
        ,a.is_netprice
        ,a.is_platform_fee
        ,a.is_creator_fee
        ,a.creator_fee_idx
        ,a.is_traded_nft
        ,a.is_moved_nft
  from seaport_ethereum_base_pairs a
  where 1=1 
    and not a.is_private
  union all
  select a.block_time
        ,a.block_number
        ,a.tx_hash
        ,a.evt_index
        ,a.sub_type
        ,a.sub_idx
        ,a.offer_first_item_type
        ,a.consideration_first_item_type
        ,a.sender
        ,case when b.tx_hash is not null then b.receiver
              else a.receiver
          end as receiver
        ,a.zone
        ,a.token_contract_address
        ,a.original_amount
        ,a.item_type
        ,a.token_id
        ,a.platform_contract_address
        ,a.offer_cnt
        ,a.consideration_cnt
        ,a.is_private
        ,a.eth_erc_idx
        ,a.nft_cnt
        ,a.erc721_cnt
        ,a.erc1155_cnt
        ,a.order_type
        ,a.is_price
        ,a.is_netprice
        ,a.is_platform_fee
        ,a.is_creator_fee
        ,a.creator_fee_idx
        ,a.is_traded_nft
        ,a.is_moved_nft
  from seaport_ethereum_base_pairs a
  left join seaport_ethereum_base_pairs b on b.tx_hash = a.tx_hash
    and b.evt_index = a.evt_index
    and b.block_time = a.block_time -- for performance
    and b.token_contract_address = a.token_contract_address
    and b.token_id = a.token_id
    and b.original_amount = a.original_amount
    and b.is_moved_nft
  where 1=1
    and a.is_private
    and not a.is_moved_nft
    and a.consideration_cnt > 0
) 
,iv_volume as (
  select block_time
        ,tx_hash
        ,evt_index
        ,max(token_contract_address) as token_contract_address 
        ,sum(case when is_price then original_amount end) as price_amount_raw
        ,sum(case when is_platform_fee then original_amount end) as platform_fee_amount_raw
        ,max(case when is_platform_fee then receiver end) as platform_fee_receiver
        ,sum(case when is_creator_fee then original_amount end) as creator_fee_amount_raw
        ,sum(case when is_creator_fee and creator_fee_idx = 1 then original_amount end) as creator_fee_amount_raw_1
        ,sum(case when is_creator_fee and creator_fee_idx = 2 then original_amount end) as creator_fee_amount_raw_2
        ,sum(case when is_creator_fee and creator_fee_idx = 3 then original_amount end) as creator_fee_amount_raw_3
        ,sum(case when is_creator_fee and creator_fee_idx = 4 then original_amount end) as creator_fee_amount_raw_4
        ,max(case when is_creator_fee and creator_fee_idx = 1 then receiver end) as creator_fee_receiver_1
        ,max(case when is_creator_fee and creator_fee_idx = 2 then receiver end) as creator_fee_receiver_2
        ,max(case when is_creator_fee and creator_fee_idx = 3 then receiver end) as creator_fee_receiver_3
        ,max(case when is_creator_fee and creator_fee_idx = 4 then receiver end) as creator_fee_receiver_4
  from iv_base_pairs_priv a
  where 1=1
    and eth_erc_idx > 0
  group by 1,2,3
)
,iv_nfts as (
  select a.block_time
        ,a.tx_hash
        ,a.evt_index
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
        ,round(price_amount_raw / nft_cnt) as price_amount_raw  -- to truncate the odd number of decimal places 
        ,round(platform_fee_amount_raw / nft_cnt) as platform_fee_amount_raw
        ,platform_fee_receiver
        ,round(creator_fee_amount_raw / nft_cnt) as creator_fee_amount_raw  
        ,creator_fee_amount_raw_1 / nft_cnt as creator_fee_amount_raw_1
        ,creator_fee_amount_raw_2 / nft_cnt as creator_fee_amount_raw_2
        ,creator_fee_amount_raw_3 / nft_cnt as creator_fee_amount_raw_3
        ,creator_fee_amount_raw_4 / nft_cnt as creator_fee_amount_raw_4
        ,creator_fee_receiver_1
        ,creator_fee_receiver_2
        ,creator_fee_receiver_3
        ,creator_fee_receiver_4
        ,case when nft_cnt > 1 then true
              else false
          end as estimated_price
        ,is_private
  from iv_base_pairs_priv a
  left join iv_volume b on b.block_time = a.block_time  -- tx_hash and evt_index is PK, but for performance, block_time is included
    and b.tx_hash = a.tx_hash
    and b.evt_index = a.evt_index
  where 1=1
    and a.is_traded_nft
)
,iv_trades as (
  select a.*
          ,try_cast(date_trunc('day', a.block_time) as date) as block_date
          ,n.name AS nft_token_name
          ,t.`from` as tx_from
          ,t.`to` as tx_to
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
          ,case when right(t.data,8) = '72db8c0b' then 'Gem'
                when right(t.data,8) = '332d1229' THEN 'Blur'
                else agg.name
           end as aggregator_name
          ,agg.contract_address AS aggregator_address
          ,'seaport-' || tx_hash || '-' || evt_index || '-' || nft_contract_address || '-' || nft_token_id as unique_trade_id
  from iv_nfts a
  inner join source_ethereum_transactions t on t.hash = a.tx_hash
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
,iv_columns as (
  -- Rename column to align other *.trades tables
  -- But the columns ordering is according to convenience.
  -- initcap the code value if needed 
  select 
    -- basic info
    'ethereum' as blockchain
    ,'seaport' as project
    ,'v1' as version

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

    -- royalty info
    ,creator_fee_receiver_1 as royalty_fee_receive_address
    ,creator_fee_amount_raw as royalty_fee_amount_raw
    ,creator_fee_amount as royalty_fee_amount
    ,creator_fee_amount_usd as royalty_fee_amount_usd
    ,creator_fee_receiver_1 as royalty_fee_receive_address_1
    ,creator_fee_receiver_2 as royalty_fee_receive_address_2
    ,creator_fee_receiver_3 as royalty_fee_receive_address_3
    ,creator_fee_receiver_4 as royalty_fee_receive_address_4
    ,creator_fee_amount_raw_1 as royalty_fee_amount_raw_1
    ,creator_fee_amount_raw_2 as royalty_fee_amount_raw_2
    ,creator_fee_amount_raw_3 as royalty_fee_amount_raw_3
    ,creator_fee_amount_raw_4 as royalty_fee_amount_raw_4

    -- aggregator
    ,aggregator_name
    ,aggregator_address

    -- tx
    ,tx_hash
    ,evt_index
    ,tx_from
    ,tx_to
    ,right_hash

    -- seaport etc
    ,zone as zone_address
    ,estimated_price
    ,is_private

    -- unique key    
    ,unique_trade_id
  from iv_trades
)
select *
from iv_columns
;