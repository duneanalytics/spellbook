{{ config(

    alias = 'base_pairs',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'sub_type', 'sub_idx'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                            "project",
                            "seaport",
                            \'["sohwak"]\') }}'
    )
}}

{% set c_seaport_first_date = '2022-06-01' %}

with iv_offer_consideration as (
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
            ,case when recipient = 0x0000000000000000000000000000000000000000 then true
                else false
            end as is_private
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
            -- , orderHash
            , recipient
            , zone
            , offer_idx
            , offer_item
        from {{ source('seaport_optimism', 'Seaport_evt_OrderFulfilled') }}
        cross join unnest(offer) with ordinality as foo(offer_item, offer_idx)
        {% if not is_incremental() %}
        where evt_block_time >= TIMESTAMP '{{c_seaport_first_date}}'  -- seaport first txn
        {% endif %}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
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
            ,case when recipient = 0x0000000000000000000000000000000000000000 then true
                else false
            end as is_private
    from
    (
        select consideration
            , contract_address
            , evt_block_number
            , evt_block_time
            , evt_index
            , evt_tx_hash
            , offer
            , recipient
            , zone
            , consideration_item
            , consideration_idx
        from {{ source('seaport_optimism','Seaport_evt_OrderFulfilled') }}
        cross join unnest(consideration) with ordinality as foo(consideration_item,consideration_idx)
        {% if not is_incremental() %}
        where evt_block_time >= TIMESTAMP '{{c_seaport_first_date}}'  -- seaport first txn
        {% endif %}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )
)
,iv_base_pairs as (
    select a.*
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
