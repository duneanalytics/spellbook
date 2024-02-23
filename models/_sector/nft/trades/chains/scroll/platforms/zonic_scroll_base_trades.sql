{{ config(
    schema = 'zonic_scroll',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set c_native_token_address = '0x0000000000000000000000000000000000000000' %}
{% set c_alternative_token_address = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' %}  -- ETH
{% set zonic_fee_address_address = '0xc353de8af2ee32da2eeae58220d3c8251ee1adcf' %}
{% set min_block_number = 72260823 %}
{% set project_start_date = '2023-02-04' %}

with events_raw as (
    select
       evt_block_time as block_time
       ,evt_block_number as block_number
       ,evt_tx_hash as tx_hash
       ,evt_index
       ,buyer
       ,offerer as seller
       ,contract_address as project_contract_address
       ,identifier as token_id
       ,token as nft_contract_address
       ,totalPrice as amount_raw
       ,marketplaceFee as platform_fee_amount_raw
       ,creatorFee as royalty_fee_amount_raw
       ,case
            when currency = {{c_native_token_address}} then {{c_alternative_token_address}}
            else currency
        end as currency_contract
       ,saleId as sale_id
    from {{ source('zonic_scroll', 'ZonicMarketplace_evt_ZonicBasicOrderFulfilled') }} as o
    {% if not is_incremental() %}
    where evt_block_time >= TIMESTAMP '{{project_start_date}}'  -- zonic first txn
    {% endif %}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
,transfers_raw as (
    -- eth royalities
    select
      tr.tx_block_number as block_number
      ,tr.tx_block_time as block_time
      ,tr.tx_hash
      ,cast(tr.value as uint256) as value
      ,tr.to
      ,er.evt_index
      ,er.evt_index - coalesce(element_at(tr.trace_address,1), 0) as ranking
    from events_raw as er
    join {{ ref('transfers_scroll_eth') }} as tr
      on er.tx_hash = tr.tx_hash
      and er.block_number = tr.tx_block_number
      and tr.value_decimal > 0
      and tr."from" in (er.project_contract_address, er.buyer) -- only include transfer from zonic or buyer to royalty fee address
      and tr.to not in (
        {{zonic_fee_address_address}} --platform fee address
        ,er.seller
        ,er.project_contract_address
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and tr.tx_block_number >= {{min_block_number}}
      {% endif %}
      {% if is_incremental() %}
      and tr.tx_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}

    union all

    -- erc20 royalities
    select
      erc20.evt_block_number as block_number
      ,erc20.evt_block_time as block_time
      ,erc20.evt_tx_hash as tx_hash
      ,erc20.value
      ,erc20.to
      ,er.evt_index
      ,er.evt_index - erc20.evt_index as ranking
    from events_raw as er
    join {{ source('erc20_scroll','evt_transfer') }} as erc20
      on er.tx_hash = erc20.evt_tx_hash
      and er.block_number = erc20.evt_block_number
      and erc20.value is not null
      and erc20."from" in (er.project_contract_address, er.buyer) -- only include transfer from zonic to royalty fee address
      and erc20.to not in (
        {{zonic_fee_address_address}} --platform fee address
        ,er.seller
        ,er.project_contract_address
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and erc20.evt_block_number >= {{min_block_number}}
      {% endif %}
      {% if is_incremental() %}
      and erc20.evt_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)
,transfers as (
    select
        block_number
        ,block_time
        ,tx_hash
        ,value
        ,to
        ,evt_index
    from (
        select
            *
            ,row_number() over (partition by tx_hash, evt_index order by abs(ranking)) as rn
        from transfers_raw
    ) as x
    where rn = 1 -- select closest by order
)
, base_trades as (
select
    'scroll' as blockchain
    ,'zonic' as project
    ,'v1' as project_version
    ,er.tx_hash
    ,er.block_number
    ,er.block_time
    ,cast(date_trunc('day', er.block_time) as date) as block_date
    ,cast(date_trunc('month', er.block_time) as date) as block_month
    ,er.token_id as nft_token_id
    ,'secondary' as trade_type
    ,uint256 '1' as nft_amount
    ,'Buy' as trade_category
    ,er.seller
    ,er.buyer
    ,er.amount_raw as price_raw
    ,er.currency_contract
    ,er.nft_contract_address
    ,er.project_contract_address
    ,er.platform_fee_amount_raw
    ,er.royalty_fee_amount_raw
    ,case when tr.value is not null then tr.to end as royalty_fee_address
    ,cast(null as varbinary) as platform_fee_address
    ,er.evt_index as sub_tx_trade_id
from events_raw as er
left join transfers as tr
    on tr.tx_hash = er.tx_hash
    and tr.block_number = er.block_number
    and tr.evt_index = er.evt_index
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'scroll') }}
