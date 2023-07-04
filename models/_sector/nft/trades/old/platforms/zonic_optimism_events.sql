{{ config(
    schema = 'zonic_optimism',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}
{% set c_native_token_address = "0x0000000000000000000000000000000000000000" %}
{% set c_alternative_token_address = "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000" %}  -- ETH
{% set zonic_fee_address_address = "0xc353de8af2ee32da2eeae58220d3c8251ee1adcf" %}
{% set c_native_symbol = "ETH" %}
{% set min_block_number = 72260823 %}
{% set project_start_date = '2023-02-04' %}

with source_optimism_transactions as (
    select *
    from {{ source('optimism','transactions') }}
    {% if not is_incremental() %}
    where block_number >= {{min_block_number}}  -- zonic first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,ref_tokens_nft as (
    select *
    from {{ ref('tokens_nft') }}
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
      and minute >= '{{project_start_date}}'  -- first txn
    {% endif %}
    {% if is_incremental() %}
      and minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,events_raw as (
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
       ,cast(totalPrice as decimal(38, 0)) as amount_raw
       ,cast(marketplaceFee as double) as platform_fee_amount_raw
       ,cast(creatorFee as double) as royalty_fee_amount_raw
       ,case
            when currency =  '{{c_native_token_address}}' then '{{c_alternative_token_address}}'
            else currency
        end as currency_contract
       ,saleId as sale_id
    from {{ source('zonic_optimism', 'ZonicMarketplace_evt_ZonicBasicOrderFulfilled') }} as o
    {% if not is_incremental() %}
    where evt_block_time >= '{{project_start_date}}'  -- zonic first txn
    {% endif %}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,transfers_raw as (
    -- eth royalities
    select
      tr.tx_block_number as block_number
      ,tr.tx_block_time as block_time
      ,tr.tx_hash
      ,tr.value
      ,tr.to
      ,er.evt_index
      ,er.evt_index - coalesce(tr.trace_address[0], 0) as ranking
    from events_raw as er
    join {{ ref('transfers_optimism_eth') }} as tr
      on er.tx_hash = tr.tx_hash
      and er.block_number = tr.tx_block_number
      and tr.value_decimal > 0
      and tr.from in (er.project_contract_address, er.buyer) -- only include transfer from zonic or buyer to royalty fee address
      and tr.to not in (
        lower('{{zonic_fee_address_address}}') --platform fee address
        ,er.seller
        ,er.project_contract_address
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and tr.tx_block_number >= {{min_block_number}}
      {% endif %}
      {% if is_incremental() %}
      and tr.tx_block_time >= date_trunc("day", now() - interval '1 week')
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
    join {{ source('erc20_optimism','evt_transfer') }} as erc20
      on er.tx_hash = erc20.evt_tx_hash
      and er.block_number = erc20.evt_block_number
      and erc20.value is not null
      and erc20.from in (er.project_contract_address, er.buyer) -- only include transfer from zonic to royalty fee address
      and erc20.to not in (
        lower('{{zonic_fee_address_address}}') --platform fee address
        ,er.seller
        ,er.project_contract_address
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and erc20.evt_block_number >= {{min_block_number}}
      {% endif %}
      {% if is_incremental() %}
      and erc20.evt_block_time >= date_trunc("day", now() - interval '1 week')
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
select
    'optimism' as blockchain
    ,'zonic' as project
    ,'v1' as version
    ,try_cast(date_trunc('day', er.block_time) as date) as block_date
    ,er.block_time
    ,er.token_id
    ,n.name as collection
    ,er.amount_raw / power(10, t1.decimals) * p1.price as amount_usd
    ,case
        when erct2.evt_tx_hash is not null then 'erc721'
        when erc1155.evt_tx_hash is not null then 'erc1155'
    end as token_standard
    ,'Single Item Trade' as trade_type
    ,cast(1 as decimal(38, 0)) as number_of_items
    ,'Buy' as trade_category
    ,'Trade' as evt_type
    ,er.seller
    ,case
        when er.buyer = agg.contract_address then coalesce(erct2.to, erc1155.to)
        else er.buyer
    end as buyer
    ,er.amount_raw / power(10, t1.decimals) as amount_original
    ,er.amount_raw
    ,t1.symbol as currency_symbol
    ,er.currency_contract
    ,er.nft_contract_address
    ,er.project_contract_address
    ,agg.name as aggregator_name
    ,agg.contract_address as aggregator_address
    ,er.tx_hash
    ,er.evt_index as evt_index
    ,er.block_number
    ,tx.from as tx_from
    ,tx.to as tx_to
    ,er.platform_fee_amount_raw
    ,er.platform_fee_amount_raw / power(10, t1.decimals) as platform_fee_amount
    ,er.platform_fee_amount_raw / power(10, t1.decimals) * p1.price as platform_fee_amount_usd
    ,cast(2.5 as double) as platform_fee_percentage
    ,er.royalty_fee_amount_raw
    ,er.royalty_fee_amount_raw / power(10, t1.decimals) as royalty_fee_amount
    ,er.royalty_fee_amount_raw / power(10, t1.decimals) * p1.price as royalty_fee_amount_usd
    ,er.royalty_fee_amount_raw / er.amount_raw * 100 as royalty_fee_percentage
    ,case when tr.value is not null then tr.to end as royalty_fee_receive_address
    ,t1.symbol as royalty_fee_currency_symbol
    ,concat(er.block_number,'-',er.tx_hash,'-',er.evt_index,'-', er.sale_id) as unique_trade_id
from events_raw as er
join source_optimism_transactions as tx
    on er.tx_hash = tx.hash
    and tx.block_number = er.block_number
left join ref_nft_aggregators as agg
    on agg.contract_address = tx.to
    and agg.blockchain = 'optimism'
left join ref_tokens_nft as n
    on n.contract_address = er.nft_contract_address
left join ref_tokens_erc20 as t1
    on t1.contract_address = er.currency_contract
left join source_prices_usd as p1
    on p1.minute = date_trunc('minute', er.block_time)
    and p1.contract_address = er.currency_contract
left join {{ source('erc721_optimism','evt_transfer') }} as erct2
    on erct2.evt_block_time=er.block_time
    and er.nft_contract_address=erct2.contract_address
    and erct2.evt_tx_hash=er.tx_hash
    and erct2.tokenId=er.token_id
    and erct2.to=er.buyer
    {% if not is_incremental() %}
    -- smallest block number for source tables above
    and erct2.evt_block_number >= {{min_block_number}}
    {% endif %}
    {% if is_incremental() %}
    and erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ source('erc1155_optimism','evt_transfersingle') }} as erc1155
    on erc1155.evt_block_time=er.block_time
    and er.nft_contract_address=erc1155.contract_address
    and erc1155.evt_tx_hash=er.tx_hash
    and erc1155.id=er.token_id
    and erc1155.to=er.buyer
    {% if not is_incremental() %}
    -- smallest block number for source tables above
    and erc1155.evt_block_number >= '{{min_block_number}}'
    {% endif %}
    {% if is_incremental() %}
    and erc1155.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join transfers as tr
    on tr.tx_hash = er.tx_hash
    and tr.block_number = er.block_number
    and tr.evt_index = er.evt_index
