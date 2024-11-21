{% macro seaport_v4_trades(
     blockchain
     ,source_transactions
     ,Seaport_evt_OrderFulfilled
     ,Seaport_evt_OrdersMatched
     ,fee_wallet_list_cte
     ,start_date = '2023-02-01'
     ,native_currency_contract = '0x0000000000000000000000000000000000000000'
     ,Seaport_order_contracts = [
        '0x00000000000001ad428e4906ae43d8f9852d0dd6'
        ,'0x00000000000000adc04c56bf30ac9d3c0aaf14dc'
        ,'0x0000000000000068F116a894984e2DB1123eB395'
     ]
     ,project = 'opensea'
     ,version = 'v4'
) %}

with source_ethereum_transactions as (
    select *
    from {{ source_transactions }}
    {% if not is_incremental() %}
    where block_time >= TIMESTAMP '{{start_date}}'  -- seaport first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
,iv_orders_matched AS (
    select om_block_time
    , om_tx_hash
    , om_evt_index
    , om_order_hash
    , min(om_order_id) as om_order_id
    from(
    select evt_block_time as om_block_time
          ,evt_tx_hash as om_tx_hash
          ,evt_index as om_evt_index
          ,om_order_id
          ,om_order_hash
      from {{ Seaport_evt_OrdersMatched }}
      cross join unnest(orderhashes) with ordinality as foo(om_order_hash,om_order_id)
      where contract_address in ({% for order_contract in Seaport_order_contracts %}
        {{order_contract}}{%- if not loop.last -%},{%- endif -%}
        {% endfor %})
    ) group by 1,2,3,4  -- deduplicate order hash re-use in advanced matching
)
,fee_wallet_list as (
    select wallet_address, wallet_name
    from {{ fee_wallet_list_cte }}
)
,iv_offer_consideration as (
    select evt_block_time as block_time
            ,evt_block_number as block_number
            ,evt_tx_hash as tx_hash
            ,evt_index
            ,'offer' as sub_type
            ,offer_idx as sub_idx
            ,case json_extract_scalar(element_at(offer,1),'$.itemType')
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end as offer_first_item_type
            ,case json_extract_scalar(element_at(consideration,1),'$.itemType')
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
            ,from_hex(json_extract_scalar(offer_item,'$.token')) as token_contract_address
            ,cast(json_extract_scalar(offer_item,'$.amount') as uint256) as original_amount
            ,case json_extract_scalar(offer_item,'$.itemType')
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end as item_type
            ,cast(json_extract_scalar(offer_item,'$.identifier') as uint256) as token_id
            ,contract_address as platform_contract_address
            ,cardinality(offer) as offer_cnt
            ,cardinality(consideration) as consideration_cnt
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
            , offer_idx
            , offer_item
        from {{ Seaport_evt_OrderFulfilled }}
        cross join unnest(offer) with ordinality as foo(offer_item, offer_idx)
        where contract_address in ({% for order_contract in Seaport_order_contracts %}
            {{order_contract}}{%- if not loop.last -%},{%- endif -%}
            {% endfor %})
        {% if not is_incremental() %}
        and evt_block_time >= TIMESTAMP '{{start_date}}'  -- seaport first txn
        {% endif %}
        {% if is_incremental() %}
        and evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )
    union all
    select evt_block_time as block_time
            ,evt_block_number as block_number
            ,evt_tx_hash as tx_hash
            ,evt_index
            ,'consideration' as sub_type
            ,consideration_idx as sub_idx
            ,case json_extract_scalar(element_at(offer,1),'$.itemType')
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end as offer_first_item_type
            ,case json_extract_scalar(element_at(consideration,1),'$.itemType')
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc'
            end as consideration_first_item_type
            ,offerer
            ,recipient
            ,recipient as sender
            ,from_hex(json_extract_scalar(consideration_item,'$.recipient')) as receiver
            ,zone
            ,from_hex(json_extract_scalar(consideration_item,'$.token')) as token_contract_address
            ,cast(json_extract_scalar(consideration_item,'$.amount') as uint256) as original_amount
            ,case json_extract_scalar(consideration_item,'$.itemType')
                when '0' then 'native'
                when '1' then 'erc20'
                when '2' then 'erc721'
                when '3' then 'erc1155'
                else 'etc' -- actually not exists
            end as item_type
            ,cast(json_extract_scalar(consideration_item,'$.identifier') as uint256) as token_id
            ,contract_address as platform_contract_address
            ,cardinality(offer) as offer_cnt
            ,cardinality(consideration) as consideration_cnt
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
            , consideration_item
            , consideration_idx
        from {{ Seaport_evt_OrderFulfilled }}
        cross join unnest(consideration) with ordinality as foo(consideration_item,consideration_idx)
        where contract_address in ({% for order_contract in Seaport_order_contracts %}
            {{order_contract}}{%- if not loop.last -%},{%- endif -%}
            {% endfor %})
        {% if not is_incremental() %}
        and evt_block_time >= TIMESTAMP '{{start_date}}'  -- seaport first txn
        {% endif %}
        {% if is_incremental() %}
        and evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )
)

,iv_base_pairs as (
    select a.block_time
            ,a.block_number
            ,a.tx_hash
            ,coalesce(a.om_evt_index, 0 ) + a.evt_index as evt_index  -- when orders_matched exists, add it to the evt_index to prevent duplication
--            ,coalesce(a.om_evt_index, a.evt_index) as evt_index
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
            ,a.om_evt_index
            ,a.om_order_id
            ,a.is_self_trans
            ,a.is_platform_fee
            ,a.eth_erc_idx
            ,a.nft_cnt
            ,a.erc721_cnt
            ,a.erc1155_cnt
            ,case when offer_first_item_type = 'erc20' then 'Sell'
                when offer_first_item_type in ('erc721','erc1155') then 'Buy'
                else 'etc' -- some txns has no nfts
            end as order_type
            ,case when om_order_id % 2 = 0 then false
                when offer_first_item_type = 'erc20' and sub_type = 'offer' and item_type = 'erc20' then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and item_type in ('native','erc20') then true
                else false
            end is_price
            ,case when om_order_id % 2 = 0 then false
                when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx = 0 then true  -- offer accepted has no price at all. it has to be calculated.
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx = 1 then true
                else false
            end is_netprice
            ,case when is_platform_fee then false
                when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx > 0 then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx > 1 then true
                when om_order_id % 2 = 0 and item_type = 'erc20' then true  -- hard code for order-matched joined additional creator fee, condition : 2nd order + erc20
                else false
            end is_creator_fee
            ,sum(case when is_platform_fee then null
                    when offer_first_item_type = 'erc20' and sub_type = 'consideration' and eth_erc_idx > 0 then 1
                    when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and eth_erc_idx > 1 then 1
                    when om_order_id % 2 = 0 and item_type = 'erc20' then 1
                end) over (partition by tx_hash, coalesce(om_evt_index,evt_index) order by evt_index, eth_erc_idx) as creator_fee_idx
            ,case when offer_first_item_type = 'erc20' and sub_type = 'consideration' and item_type in ('erc721','erc1155') then true
                when offer_first_item_type in ('erc721','erc1155') and sub_type = 'offer' and item_type in ('erc721','erc1155') then true
                else false
            end is_traded_nft
            ,case when offer_first_item_type in ('erc721','erc1155') and sub_type = 'consideration' and item_type in ('erc721','erc1155') then true
                else false
            end is_moved_nft
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
                ,b.om_evt_index
                ,b.om_order_id
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
                    left join fee_wallet_list f on f.wallet_address = a.receiver
                    left join iv_orders_matched b on b.om_tx_hash = a.tx_hash
                                                    and b.om_order_hash = a.order_hash
          ) a
    where not is_self_trans
)
,iv_volume as (
  select block_time
        ,tx_hash
        ,evt_index
        ,max(token_contract_address) as token_contract_address
        ,CAST(sum(case when is_price then original_amount end) AS DOUBLE) as price_amount_raw
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
  group by 1,2,3
  having count(distinct token_contract_address) = 1  -- some private sale trade has more that one currencies
)
,iv_nfts as (
  select a.block_time
        ,a.tx_hash
        ,a.evt_index
        ,a.block_number
        ,a.sender as seller
        ,a.receiver as buyer
        ,'secondary' as trade_type
        ,a.order_type
        ,a.token_contract_address as nft_contract_address
        ,a.original_amount as nft_token_amount
        ,a.token_id as nft_token_id
        ,a.item_type as nft_token_standard
        ,a.zone
        ,a.platform_contract_address
        ,b.token_contract_address
        ,CAST(round(price_amount_raw / nft_cnt) as uint256) as price_amount_raw  -- to truncate the odd number of decimal places
        ,cast(platform_fee_amount_raw / nft_cnt as uint256) as platform_fee_amount_raw
        ,platform_fee_receiver
        ,cast(creator_fee_amount_raw / nft_cnt as uint256) as creator_fee_amount_raw
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
        left join iv_volume b on b.block_time = a.block_time  -- tx_hash and evt_index is PK, but for performance, block_time is included
                              and b.tx_hash = a.tx_hash
                              and b.evt_index = a.evt_index
  where 1=1
    and a.is_traded_nft
)
,iv_trades as (
    select a.block_time
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
          ,t."from" as tx_from
          ,t.to as tx_to
          ,bytearray_reverse(bytearray_substring(bytearray_reverse(t.data),1,4)) as right_hash
          ,bytearray_reverse(bytearray_substring(bytearray_reverse(t.data),1,32))  as tx_data_marker
          ,a.fee_wallet_name
  from iv_nfts a
  inner join source_ethereum_transactions t on t.hash = a.tx_hash
  where t."from" != 0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd -- this is a special address which transact English Auction, will handle later.

)
  -- Rename column to align other *.trades tables
  -- But the columns ordering is according to convenience.
  -- initcap the code value if needed
select
        -- basic info
         '{{blockchain}}' as blockchain
        ,'{{project}}' as project
        ,'{{version}}' as project_version

        -- order info
        ,block_time
        ,cast(date_trunc('day', block_time) as date) as block_date
        ,cast(date_trunc('month', block_time) as date) as block_month
        ,seller
        ,buyer
        ,trade_type
        ,order_type as trade_category -- Buy / Sell

        -- nft token info
        ,nft_contract_address
        ,nft_token_id as nft_token_id
        ,nft_token_amount as nft_amount

        -- price info
        ,price_amount_raw as price_raw
        ,case when token_contract_address = 0x0000000000000000000000000000000000000000 then {{native_currency_contract}}
         else token_contract_address end as currency_contract

        -- project info (platform or exchange)
        ,platform_contract_address as project_contract_address
        ,platform_fee_receiver as platform_fee_address
        ,platform_fee_amount_raw

        -- royalty info
        ,creator_fee_receiver_1 as royalty_fee_address
        ,creator_fee_amount_raw as royalty_fee_amount_raw

        -- tx
        ,block_number
        ,tx_hash
        ,tx_from
        ,tx_to
        ,tx_data_marker

        -- seaport etc
        , row_number() over (partition by tx_hash order by evt_index) as sub_tx_trade_id

        ,right_hash
        ,fee_wallet_name
        ,zone as zone_address
  from   iv_trades

{% endmacro %}
