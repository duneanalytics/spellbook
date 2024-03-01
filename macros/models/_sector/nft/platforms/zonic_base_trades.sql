-- Zonic Marketplace NFT trades (re-usable macro for all chains)
{%
    macro zonic_base_trades(
        blockchain,
        min_block_number,
        project_start_date,
        c_native_token_address = '0x0000000000000000000000000000000000000000',
        c_alternative_token_address = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000',
        zonic_fee_address_address = '0xc353de8af2ee32da2eeae58220d3c8251ee1adcf',
        project_decoded_as = 'zonic'
    )
%}

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
    from {{ source(project_decoded_as ~ '_' ~ blockchain, 'ZonicMarketplace_evt_ZonicBasicOrderFulfilled') }} as o
    {% if not is_incremental() %}
    where evt_block_time >= timestamp '{{project_start_date}}'  -- zonic first txn
    {% endif %}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}
)
,transfers_raw as (
    -- royalties
    select
      tr.block_number
      ,tr.block_time
      ,tr.tx_hash
      ,tr.amount_raw as value
      ,tr.to
      ,er.evt_index
      ,er.evt_index - coalesce(tr.evt_index,element_at(tr.trace_address,1), 0) as ranking
    from events_raw as er
    join {{ ref('tokens_' ~ blockchain ~ '_base_transfers') }} as tr
      on er.tx_hash = tr.tx_hash
      and er.block_number = tr.block_number
      and tr.amount_raw > 0
      and tr."from" in (er.project_contract_address, er.buyer) -- only include transfer from zonic or buyer to royalty fee address
      and tr.to not in (
        {{zonic_fee_address_address}} --platform fee address
        ,er.seller
        ,er.project_contract_address
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and tr.block_number >= {{min_block_number}}
      {% endif %}
      {% if is_incremental() %}
      and {{incremental_predicate('tr.block_time')}}
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
    '{{ blockchain }}' as blockchain
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
{{ add_nft_tx_data('base_trades', blockchain) }}

{% endmacro %}
