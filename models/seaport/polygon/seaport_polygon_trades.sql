{{ config(
    
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                            "project",
                            "seaport",
                            \'["sohawk"]\') }}'
    )
}}

{% set c_native_token_address = '0x0000000000000000000000000000000000000000' %}
{% set c_alternative_token_address = '0x0000000000000000000000000000000000001010' %}  -- MATIC
{% set c_native_symbol = 'MATIC' %}
{% set c_seaport_first_date = '2022-06-01' %}

with source_polygon_transactions as (
    select *
    from {{ source('polygon','transactions') }}
    {% if not is_incremental() %}
    where block_time >= TIMESTAMP '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
,ref_seaport_polygon_base_pairs as (
      select *
      from {{ ref('seaport_polygon_base_pairs') }}
      where 1=1
      {% if is_incremental() %}
            and block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)
,ref_tokens_nft as (
    select *
    from {{ ref('tokens_nft') }}
    where blockchain = 'polygon'
)
,ref_tokens_erc20 as (
    select *
    from {{ source('tokens', 'erc20') }}
    where blockchain = 'polygon'
)
,ref_nft_aggregators as (
    select *
    from {{ ref('nft_aggregators') }}
    where blockchain = 'polygon'
)
,source_prices_usd as (
    select *
    from {{ source('prices', 'usd') }}
    where blockchain = 'polygon'
    {% if not is_incremental() %}
      and minute >= TIMESTAMP '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
      and minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
,iv_base_pairs_priv as (
  select a.block_date
        ,a.block_time
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
  from ref_seaport_polygon_base_pairs a
  where 1=1
    and not a.is_private
  union all
  select distinct -- temporary solution to https://github.com/duneanalytics/spellbook/issues/2404
         a.block_date
        ,a.block_time
        ,a.block_number
        ,a.tx_hash
        ,a.evt_index
        ,a.sub_type
        ,coalesce(b.sub_idx, a.sub_idx) as sub_idx -- temporary solution to #2404
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
  from ref_seaport_polygon_base_pairs a
  left join ref_seaport_polygon_base_pairs b on b.tx_hash = a.tx_hash
    and b.evt_index = a.evt_index
    and b.block_date = a.block_date -- for performance
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
  select block_date
        ,block_time
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
  group by 1,2,3,4
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
        ,CAST( price_amount_raw / nft_cnt as uint256) as price_amount_raw  -- to truncate the odd number of decimal places
        ,cast(platform_fee_amount_raw / nft_cnt as uint256) as platform_fee_amount_raw
        ,platform_fee_receiver
        ,cast(creator_fee_amount_raw / nft_cnt as uint256) as creator_fee_amount_raw
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
        ,sub_type
        ,sub_idx
  from iv_base_pairs_priv a
  left join iv_volume b on b.block_date = a.block_date  -- tx_hash and evt_index is PK, but for performance, block_time is included
    and b.tx_hash = a.tx_hash
    and b.evt_index = a.evt_index
  where 1=1
    and a.is_traded_nft
)
,iv_trades as (
  select a.*
          ,n.name AS nft_token_name
          ,t."from" as tx_from
          ,t.to as tx_to
          ,bytearray_reverse(bytearray_substring(bytearray_reverse(t.data),1,4)) as right_hash
          ,case when a.token_contract_address = {{c_native_token_address}} then '{{c_native_symbol}}'
                else e.symbol
           end as token_symbol
          ,case when a.token_contract_address = {{c_native_token_address}} then {{c_alternative_token_address}}
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
  from iv_nfts a
  inner join source_polygon_transactions t on t.hash = a.tx_hash
  left join ref_tokens_nft n on n.contract_address = nft_contract_address
  left join ref_tokens_erc20 e on e.contract_address = case when a.token_contract_address = {{c_native_token_address}} then {{c_alternative_token_address}}
                                                            else a.token_contract_address
                                                      end
  left join source_prices_usd p on p.contract_address = case when a.token_contract_address = {{c_native_token_address}} then {{c_alternative_token_address}}
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
    'polygon' as blockchain
    ,'seaport' as project
    ,'v1' as version

    -- order info
    ,block_date
    ,cast(date_trunc('month',block_time) as date) as block_month
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
  from iv_trades
)
select *
from iv_columns

