{{ config(
    schema = 'nftearth_optimism',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}

{% set c_native_token_address = "0x0000000000000000000000000000000000000000" %}
{% set c_alternative_token_address = "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000" %}  -- ETH
{% set c_native_symbol = "ETH" %}
{% set c_seaport_first_date = "2023-01-31" %}
{% set non_buyer_address = "0x2140ea50bc3b6ac3971f9e9ea93a1442665670e4" %} -- nftearth contract address

with source_optimism_transactions as (
    select *
    from {{ source('optimism','transactions') }}
    {% if not is_incremental() %}
    where block_time >= '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,ref_nftearth_optimism_base_pairs as (
      select *
      from {{ ref('nftearth_optimism_base_pairs') }}
      where 1=1
      {% if is_incremental() %}
            and block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
)
,ref_tokens_nft as (
    select *
    from {{ ref('tokens_nft_legacy') }}
    where blockchain = 'optimism'
)
,ref_tokens_erc20 as (
    select *
    from {{ ref('tokens_erc20_legacy') }}
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
      and minute >= '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
      and minute >= date_trunc("day", now() - interval '1 week')
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
  from ref_nftearth_optimism_base_pairs a
  where 1=1
    and not a.is_private
  union all
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
  from ref_nftearth_optimism_base_pairs a
  left join ref_nftearth_optimism_base_pairs b on b.tx_hash = a.tx_hash
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
,erc721_transfer as (
  select
    evt_tx_hash
    ,evt_block_time
    ,evt_block_number
    ,tokenId
    ,contract_address
    ,from
    ,to
  from {{ source('erc721_optimism','evt_transfer') }}
  where
    (from = '{{non_buyer_address}}'
    or to = '{{non_buyer_address}}')
    {% if not is_incremental() %}
    and evt_block_time >= '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    and evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,erc1155_transfer as (
  select
    evt_tx_hash
    ,evt_block_time
    ,evt_block_number
    ,id as tokenId
    ,contract_address
    ,from
    ,to
  from {{ source('erc1155_optimism','evt_transfersingle') }}
  where
    (from = '{{non_buyer_address}}'
    or to = '{{non_buyer_address}}')
    {% if not is_incremental() %}
    and evt_block_time >= '{{c_seaport_first_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    and evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,all_transfers as (
  select *
  from erc721_transfer

  union all

  select *
  from erc1155_transfer
)
,iv_columns as (
  -- Rename column to align other *.trades tables
  -- But the columns ordering is according to convenience.
  -- initcap the code value if needed
  select
    -- basic info
    'optimism' as blockchain
    ,'nftearth' as project
    ,'v1' as version

    -- order info
    ,t.block_date
    ,t.block_time
    ,case when t.seller = '{{non_buyer_address}}' then erc2.from else t.seller end as seller
    ,case when t.buyer = '{{non_buyer_address}}' then erc.to else t.buyer end as buyer
    ,initcap(t.trade_type) as trade_type
    ,initcap(t.order_type) as trade_category -- Buy / Offer Accepted
    ,'Trade' as evt_type

    -- nft token info
    ,t.nft_contract_address
    ,t.nft_token_name as collection
    ,t.nft_token_id as token_id
    ,cast(t.nft_token_amount as decimal(38, 0)) as number_of_items
    ,t.nft_token_standard as token_standard

    -- price info
    ,t.price_amount as amount_original
    ,cast(t.price_amount_raw as decimal(38, 0)) as amount_raw
    ,t.price_amount_usd as amount_usd
    ,t.token_symbol as currency_symbol
    ,t.token_alternative_symbol as currency_contract
    ,t.token_contract_address as original_currency_contract
    ,t.price_token_decimals as currency_decimals   -- in case calculating royalty1~4

    -- project info (platform or exchange)
    ,t.platform_contract_address as project_contract_address
    ,t.platform_fee_receiver as platform_fee_receive_address
    ,CAST(t.platform_fee_amount_raw as double) as platform_fee_amount_raw
    ,t.platform_fee_amount
    ,t.platform_fee_amount_usd
    ,case when t.price_amount_raw > 0 then CAST ((t.platform_fee_amount_raw / t.price_amount_raw * 100) AS DOUBLE) end platform_fee_percentage

    -- royalty info
    ,t.creator_fee_receiver_1 as royalty_fee_receive_address
    ,CAST(t.creator_fee_amount_raw as double) as royalty_fee_amount_raw
    ,case when t.price_amount_raw > 0 then CAST ((creator_fee_amount_raw / t.price_amount_raw * 100) AS DOUBLE) end royalty_fee_percentage
    ,t.token_symbol as royalty_fee_currency_symbol
    ,t.creator_fee_amount as royalty_fee_amount
    ,t.creator_fee_amount_usd as royalty_fee_amount_usd
    ,t.creator_fee_receiver_1 as royalty_fee_receive_address_1
    ,t.creator_fee_receiver_2 as royalty_fee_receive_address_2
    ,t.creator_fee_receiver_3 as royalty_fee_receive_address_3
    ,t.creator_fee_receiver_4 as royalty_fee_receive_address_4
    ,t.creator_fee_amount_raw_1 as royalty_fee_amount_raw_1
    ,t.creator_fee_amount_raw_2 as royalty_fee_amount_raw_2
    ,t.creator_fee_amount_raw_3 as royalty_fee_amount_raw_3
    ,t.creator_fee_amount_raw_4 as royalty_fee_amount_raw_4

    -- aggregator
    ,t.aggregator_name
    ,t.aggregator_address

    -- tx
    ,t.block_number
    ,t.tx_hash
    ,t.evt_index
    ,t.tx_from
    ,t.tx_to
    ,t.right_hash

    -- seaport etc
    ,t.zone as zone_address
    ,t.estimated_price
    ,t.is_private
    ,t.sub_idx
    ,t.sub_type
  from iv_trades as t
  left join all_transfers as erc
    on t.tx_hash = erc.evt_tx_hash
    and t.block_number = erc.evt_block_number
    and t.nft_token_id = erc.tokenId
    and t.nft_contract_address = erc.contract_address
    and t.buyer = erc.from
  left join all_transfers as erc2
    on t.tx_hash = erc2.evt_tx_hash
    and t.block_number = erc2.evt_block_number
    and t.nft_token_id = erc2.tokenId
    and t.nft_contract_address = erc2.contract_address
    and t.seller = erc2.to
)
select
  *
  ,concat(block_date, tx_hash, evt_index, nft_contract_address, token_id, sub_type, sub_idx) as unique_trade_id
from iv_columns
