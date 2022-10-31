{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index'],
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
    --   from ethereum.transactions 
      from {{ source('ethereum','transactions') }}
     where block_time > date '{{c_seaport_first_date}}'  -- seaport first txn
    {% if is_incremental() %}
      and t.block_time >= date_trunc('day', current_date - interval '1 week')
    {% endif %}
)
,ref_seaport_ethereum_base_pairs as (
    select *
    --   from seaport_ethereum_base_pairs
      from {{ ref('seaport_ethereum_base_pairs') }}
     where 1=1
     {% if is_incremental() %}
      and block_time >= date_trunc('day', current_date - interval '1 week')
     {% endif %}     
)
,ref_tokens_nft as (
    select *
    --   from tokens.nft
      from {{ ref('tokens_nft') }}
     where blockchain = 'ethereum'
)
,ref_tokens_erc20 as (
    select *
    --   from tokens.erc20
      from {{ ref('tokens_erc20') }}
     where blockchain = 'ethereum'
)
,source_prices_usd as (
    select *
    --   from prices.usd
      from {{ source('prices', 'usd') }}
     where blockchain = 'ethereum'
     {% if is_incremental() %} 
      and minute >= date_trunc('day', current_date - interval '1 week')
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
      from ref_seaport_ethereum_base_pairs a
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
      from ref_seaport_ethereum_base_pairs a
           left join ref_seaport_ethereum_base_pairs b on b.tx_hash = a.tx_hash
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
          ,sum(case when is_creator_fee and creator_fee_idx = 1 then original_amount end) as creator_fee_amount_1_raw
          ,sum(case when is_creator_fee and creator_fee_idx = 2 then original_amount end) as creator_fee_amount_2_raw
          ,sum(case when is_creator_fee and creator_fee_idx = 3 then original_amount end) as creator_fee_amount_3_raw
          ,sum(case when is_creator_fee and creator_fee_idx = 4 then original_amount end) as creator_fee_amount_4_raw
          ,max(case when is_creator_fee and creator_fee_idx = 1 then receiver end) as creator_fee_receiver_1_raw
          ,max(case when is_creator_fee and creator_fee_idx = 2 then receiver end) as creator_fee_receiver_2_raw
          ,max(case when is_creator_fee and creator_fee_idx = 3 then receiver end) as creator_fee_receiver_3_raw
          ,max(case when is_creator_fee and creator_fee_idx = 4 then receiver end) as creator_fee_receiver_4_raw
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
                else 'single trade'
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
          ,creator_fee_amount_1_raw / nft_cnt as creator_fee_amount_1_raw
          ,creator_fee_amount_2_raw / nft_cnt as creator_fee_amount_2_raw
          ,creator_fee_amount_3_raw / nft_cnt as creator_fee_amount_3_raw
          ,creator_fee_amount_4_raw / nft_cnt as creator_fee_amount_4_raw
          ,creator_fee_receiver_1_raw
          ,creator_fee_receiver_2_raw
          ,creator_fee_receiver_3_raw
          ,creator_fee_receiver_4_raw
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
,iv_trades(
    select a.*
          ,t.`from` as tx_from
          ,t.`to` as tx_to
          ,right(t.data,8) as right_hash
          ,case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_native_symbol}}'
                else e.symbol
           end as token_symbol
          ,a.price_amount_raw / power(10, e.decimals) as price_amount
          ,a.price_amount_raw / power(10, e.decimals) * p.price as price_amount_usd
          ,a.platform_fee_amount_raw / power(10, e.decimals) as platform_fee_amount
          ,a.platform_fee_amount_raw / power(10, e.decimals) * p.price as platform_fee_amount_usd
          ,a.creator_fee_amount_raw / power(10, e.decimals) as creator_fee_amount
          ,a.creator_fee_amount_raw / power(10, e.decimals) * p.price as creator_fee_amount_usd
          ,tx_hash || '-' || evt_index as unique_trade_id
      from iv_nfts a
           inner join source_ethereum_transactions t on t.hash = a.tx_hash
           left join ref_tokens_nft n on n.contract_address = nft_contract_address 
           left join ref_tokens_erc20 e on e.contract_address = a.token_contract_address
           left join source_prices_usd p on p.contract_address = case when a.token_contract_address = '{{c_native_token_address}}' then '{{c_alternative_token_address}}'
                                                                      else a.token_contract_address
                                                                 end
                                         and p.minute = date_trunc('minute', a.block_time)
)
select *
  from iv_trades