{{ config(
    
    schema = 'seaport_v2_ethereum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "seaport_v2",
                            \'["sohwak"]\') }}'
    )
}}

{% set c_native_token_address = '0x0000000000000000000000000000000000000000' %}
{% set c_alternative_token_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" %}
{% set c_native_symbol = 'ETH' %}
{% set c_seaport_first_date = "2023-02-01" %}


with source_ethereum_transactions as (
    select *
    from {{ source('ethereum','transactions') }}
    {% if not is_incremental() %}
    where block_time >= TIMESTAMP '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
,ref_seaport_ethereum_base_pairs as (
      select *
      from {{ ref('seaport_ethereum_base_pairs') }}
      where 1=1
      {% if is_incremental() %}
            and block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)
,ref_tokens_nft as (
    select *
    from {{ ref('tokens_nft') }}
    where blockchain = 'ethereum'
)
,ref_tokens_erc20 as (
    select *
    from {{ source('tokens', 'erc20') }}
    where blockchain = 'ethereum'
)
,ref_nft_aggregators as (
    select *
    from {{ ref('nft_aggregators') }}
    where blockchain = 'ethereum'
)
,ref_nft_aggregators_marks as (
    select *
    from {{ ref('nft_ethereum_aggregators_markers') }}
)
,source_prices_usd as (
    select *
    from {{ source('prices', 'usd') }}
    where blockchain = 'ethereum'
    {% if not is_incremental() %}
      and minute >= TIMESTAMP '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
      and minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
,iv_orders_matched AS (
    select evt_block_time as om_block_time
          ,evt_tx_hash as om_tx_hash
          ,evt_index as om_evt_index
          ,om_order_id
          ,om_order_hash
          ,cardinality(orderHashes) as om_cnt
      from {{ source('seaport_ethereum','Seaport_evt_OrdersMatched') }}
      cross join unnest(orderHashes) with ordinality as foo(om_order_hash, om_order_id)
     where contract_address in (0x00000000000001ad428e4906ae43d8f9852d0dd6 -- Seaport v1.4
                               ,0x00000000000000adc04c56bf30ac9d3c0aaf14dc -- Seaport v1.5
                               )
)
,iv_enh_base_pairs as (
    select a.block_date
          ,a.block_time
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
          ,CAST(a.original_amount AS UINT256) AS original_amount
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
          ,a.order_hash
          ,b.om_evt_index
          ,b.om_order_id
      from ref_seaport_ethereum_base_pairs a
           left join iv_orders_matched b on b.om_order_hash = a.order_hash
                                         and b.om_tx_hash = a.tx_hash  -- order_hash is not unique in itself, so must join with tx_hash
                                         and b.om_cnt = 2
     where a.platform_contract_address in (0x00000000000001ad428e4906ae43d8f9852d0dd6 -- Seaport v1.4
                                          ,0x00000000000000adc04c56bf30ac9d3c0aaf14dc -- Seaport v1.5
                                          )
)
,iv_volume as (
  select block_date
        ,block_time
        ,tx_hash
        ,evt_index
        ,max(token_contract_address) as token_contract_address
        ,CAST(sum(case when is_price then original_amount end) AS UINT256) as price_amount_raw
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
   from iv_enh_base_pairs a
  where 1=1
    and eth_erc_idx > 0
    and NOT(om_evt_index is not null and offerer = recipient) -- for matchAdvancedOrders/matchOrders
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
        ,CAST(price_amount_raw / nft_cnt as uint256) as price_amount_raw  -- to truncate the odd number of decimal places
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
        ,order_hash
  from iv_enh_base_pairs a
        left join iv_volume b on b.block_date = a.block_date  -- tx_hash and evt_index is PK, but for performance, block_time is included
                              and b.tx_hash = a.tx_hash
                              and b.evt_index = a.evt_index
  where 1=1
    and a.is_traded_nft
    and NOT(om_evt_index is not null and offerer = recipient) -- for matchAdvancedOrders/matchOrders
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
          ,a.creator_fee_receiver_1
          ,a.creator_fee_receiver_2
          ,a.creator_fee_receiver_3
          ,a.creator_fee_receiver_4
          ,a.estimated_price
          ,a.is_private
          ,a.sub_type
          ,a.sub_idx
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
          ,coalesce(agg.name,agg_m.aggregator_name) as aggregator_name
          ,agg.contract_address AS aggregator_address
  from iv_nfts a
  inner join source_ethereum_transactions t on t.hash = a.tx_hash
  left join ref_tokens_nft n on n.contract_address = nft_contract_address
  left join ref_tokens_erc20 e on e.contract_address = case when a.token_contract_address = {{c_native_token_address}} then {{c_alternative_token_address}}
                                                            else a.token_contract_address
                                                      end
  left join source_prices_usd p on p.contract_address = case when a.token_contract_address = {{c_native_token_address}} then {{c_alternative_token_address}}
                                                            else a.token_contract_address
                                                        end
    and p.minute = date_trunc('minute', a.block_time)
  left join ref_nft_aggregators agg on agg.contract_address = t.to
  left join ref_nft_aggregators_marks agg_m on bytearray_starts_with(bytearray_reverse(t.data), bytearray_reverse(agg_m.hash_marker))
)
  -- Rename column to align other *.trades tables
  -- But the columns ordering is according to convenience.
  -- initcap the code value if needed
  select
        -- basic info
        'ethereum' as blockchain
        ,'seaport' as project
        ,'v2' as version

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
      -- where tx_hash not in ('0xff6ab6d78a69bd839ac4fa9e9347367075f3ba2d83216c561010f94291d0118c', '0x3ffc50795ecaf51f14a330605d44e41e3aa3515326560037b61edb8990ff80e2')

