-- Zonic Marketplace NFT trades (re-usable macro for all chains)
{%
    macro zonic_base_trades(
        blockchain,
        min_block_number,
        project_start_date,
        c_native_token_address = '0x0000000000000000000000000000000000000000',
        c_alternative_token_address = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000',
        zonic_fee_address_address = '0xc353de8af2ee32da2eeae58220d3c8251ee1adcf',
        project_decoded_as = 'zonic',
        royalty_fee_receive_address_to_skip = []
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
    ,cast(null as varbinary) royalty_fee_address
    ,cast(null as varbinary) as platform_fee_address
    ,er.evt_index as sub_tx_trade_id
from events_raw as er
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', blockchain) }}

{% endmacro %}
