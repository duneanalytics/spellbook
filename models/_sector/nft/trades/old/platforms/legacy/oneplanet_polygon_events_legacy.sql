{{ config(
	tags=['legacy'],
	
    schema = 'oneplanet_polygon',
    alias = alias('events', legacy_model=True),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'token_id']
    )
}}

{% set c_native_token_address = "0x0000000000000000000000000000000000000000" %}
{% set c_alternative_token_address = "0x0000000000000000000000000000000000001010" %}  -- MATIC
{% set c_native_symbol = "MATIC" %}
{% set c_oneplanet_first_date = "2023-09-03" %}

with source_polygon_transactions as (
    select block_time, block_number, "from", "to", hash, data
    from {{ source('polygon','transactions') }}
    {% if not is_incremental() %}
    where block_time >= date '{{c_oneplanet_first_date}}'
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,ref_oneplanet_polygon_base_pairs as (
      select *
      from {{ ref('oneplanet_polygon_base_pairs_legacy') }}
      where 1=1
      {% if is_incremental() %}
            and block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
)
,ref_tokens_nft as (
    select *
    from {{ ref('tokens_nft_legacy') }}
    where blockchain = 'polygon'
)
,ref_tokens_erc20 as (
    select *
    from {{ ref('tokens_erc20_legacy') }}
    where blockchain = 'polygon'
)
,ref_nft_aggregators as (
    select *
    from {{ ref('nft_aggregators_legacy') }}
    where blockchain = 'polygon'
)
,source_prices_usd as (
    select *
    from {{ source('prices', 'usd') }}
    where blockchain = 'polygon'
    {% if not is_incremental() %}
      and minute >= date '{{c_oneplanet_first_date}}'
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
  from ref_oneplanet_polygon_base_pairs a
  where 1=1
    and not a.is_private
  union all
  select distinct
         a.block_date
        ,a.block_time
        ,a.block_number
        ,a.tx_hash
        ,a.evt_index
        ,a.sub_type
        ,coalesce(b.sub_idx, a.sub_idx) as sub_idx
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
  from ref_oneplanet_polygon_base_pairs a
  left join ref_oneplanet_polygon_base_pairs b on b.tx_hash = a.tx_hash
    and b.evt_index = a.evt_index
    and b.block_date = a.block_date
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
        ,price_amount_raw
        ,platform_fee_amount_raw
        ,platform_fee_receiver
        ,creator_fee_amount_raw
        ,creator_fee_amount_raw_1
        ,creator_fee_amount_raw_2
        ,creator_fee_amount_raw_3
        ,creator_fee_amount_raw_4
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
  left join iv_volume b on b.block_date = a.block_date
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
          ,cast(coalesce(a.platform_fee_amount_raw, 0) / a.price_amount_raw as double) as platform_fee_percentage
          ,a.creator_fee_amount_raw as royalty_fee_amount_raw
          ,a.creator_fee_amount_raw / power(10, e.decimals) as royalty_fee_amount
          ,a.creator_fee_amount_raw / power(10, e.decimals) * p.price as royalty_fee_amount_usd
          ,cast(coalesce(a.creator_fee_amount_raw, 0) / a.price_amount_raw as double) as royalty_fee_percentage
          ,creator_fee_receiver_1 as royalty_fee_receive_address
          ,agg.name as aggregator_name
          ,agg.contract_address AS aggregator_address
          ,sub_idx
  from iv_nfts a
  inner join source_polygon_transactions t on t.hash = a.tx_hash
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
  select
    'polygon' as blockchain
    ,'oneplanet' as project
    ,'v1' as version
    ,block_time
    ,nft_token_id as token_id
    ,nft_token_name as collection
    ,cast(price_amount_usd as double) as amount_usd
    ,nft_token_standard as token_standard
    ,initcap(trade_type) as trade_type
    ,cast(nft_token_amount as decimal(38,0)) as number_of_items
    ,initcap(order_type) as trade_category
    ,'Trade' as evt_type
    ,seller
    ,buyer
    ,cast(price_amount as double) as amount_original
    ,cast(price_amount_raw as decimal(38,0)) as amount_raw
    ,token_symbol as currency_symbol
    ,token_alternative_symbol as currency_contract
    ,nft_contract_address
    ,platform_contract_address as project_contract_address
    ,aggregator_name
    ,aggregator_address
    ,block_number
    ,tx_hash
    ,tx_from
    ,tx_to
    ,evt_index
    ,cast(platform_fee_amount_raw as double) as platform_fee_amount_raw
    ,cast(platform_fee_amount as double) as platform_fee_amount
    ,cast(platform_fee_amount_usd as double) as platform_fee_amount_usd
    ,cast(platform_fee_percentage as double) as platform_fee_percentage
    ,cast(royalty_fee_amount_raw as double) as royalty_fee_amount_raw
    ,cast(royalty_fee_amount as double) as royalty_fee_amount
    ,cast(royalty_fee_amount_usd as double) as royalty_fee_amount_usd
    ,cast(royalty_fee_percentage as double) as royalty_fee_percentage
    ,royalty_fee_receive_address
    ,token_symbol as royalty_fee_currency_symbol
    ,'OnePlanet-' || tx_hash || '-' || cast(evt_index as VARCHAR(10)) || '-' || nft_contract_address || '-' || cast(nft_token_id as VARCHAR(10)) || '-' || cast(sub_idx as VARCHAR(10)) as unique_trade_id
  from iv_trades
)
select *
from iv_columns
;
