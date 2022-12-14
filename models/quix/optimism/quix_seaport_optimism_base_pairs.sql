{{ config(
    alias = 'base_pairs',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'sub_type', 'sub_idx'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                            "project",
                            "quix",
                            \'["chuxin"]\') }}'
    )
}}

{% set c_seaport_first_date = "2022-07-29" %}

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
            ,case when recipient = '0x0000000000000000000000000000000000000000' then true
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
            , posexplode(offer) as (offer_idx, offer_item)
        from {{ source('quixotic_optimism', 'Seaport_evt_OrderFulfilled') }}
        {% if not is_incremental() %}
        where evt_block_time >= '{{c_seaport_first_date}}'  -- seaport first txn
        {% endif %}
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
            ,case when recipient = '0x0000000000000000000000000000000000000000' then true
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
            ,posexplode(consideration) as (consideration_idx, consideration_item)
        from {{ source('quixotic_optimism', 'Seaport_evt_OrderFulfilled') }}
        {% if not is_incremental() %}
        where evt_block_time >= '{{c_seaport_first_date}}'  -- seaport first txn
        {% endif %}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    )
)