{{ config(
    schema = 'liquidifty_ethereum',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    alias = 'base_trades',

)}}

{% set weth_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" %}

with v2 as (
    select
        'ethereum' as blockchain,
        'liquidifty' as project,
        'v2' as project_version,
        evt_block_time as block_time,
        cast(token as uint256) as nft_token_id,
        'secondary' as trade_type,
        CAST(amount as uint256) as nft_amount,
        'Buy' as trade_category,
        owner as seller,
        buyer,
        price as price_raw,
        case
            when currency = 0x0000000000000000000000000000000000000000 then {{ weth_address }}
            else currency
        end as currency_contract,
        collection as nft_contract_address,
        contract_address  as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        cast(null as uint256) as platform_fee_amount_raw,
        cast(null as uint256) as royalty_fee_amount_raw,
        cast(null as varbinary) as platform_fee_address,
        cast(null as varbinary) as royalty_fee_address,
        evt_index as sub_tx_trade_id
    from {{ source('liquidifty_ethereum', 'MarketplaceV2_5_evt_Buy') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}
),
stack as (
    select
        'ethereum' as blockchain,
        'liquidifty' as project,
        'v2' as project_version,
        evt_block_time as block_time,
        cast(null as uint256) as nft_token_id,
        'secondary' as trade_type,
        CAST(amount as uint256) as nft_amount,
        'Buy' as trade_category,
        owner as seller,
        buyer,
        price as price_raw,
        case
            when currency = 0x0000000000000000000000000000000000000000 then {{ weth_address }}
            else currency
        end as currency_contract,
        collection as nft_contract_address,
        contract_address  as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        cast(null as uint256) as platform_fee_amount_raw,
        cast(null as uint256) as royalty_fee_amount_raw,
        cast(null as varbinary) as platform_fee_address,
        cast(null as varbinary) as royalty_fee_address,
        evt_index as sub_tx_trade_id
    from {{ source('liquidifty_ethereum', 'PoolSell_evt_Buy') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}
),
v3 as (
    select
        'ethereum' as blockchain,
        'liquidifty' as project,
        'v3' as project_version,
        call_block_time as block_time,
        cast(json_extract_scalar(nft, '$.id') as uint256) as nft_token_id,
        'secondary' as trade_type,
        CAST(JSON_EXTRACT_SCALAR(nft, '$.amount') as uint256) as nft_amount,
        case
            when orderType = '0' then 'Swap'
            when orderType = '1' then 'Buy'
            when orderType = '2' then 'Sell'
        end as trade_category,
        from_hex(json_extract_scalar(_order, '$.signer')) as seller,
        cast(null as varbinary) as buyer,   --todo: replace with tx_from
        cast(json_extract_scalar(currency, '$.amount') as uint256) as price_raw,
        case
            when json_extract_scalar(currency, '$.assetType') = '0' then {{ weth_address }}
            else from_hex(json_extract_scalar(currency, '$.collection'))
        end as currency_contract,
        from_hex(json_extract_scalar(nft, '$.collection')) as nft_contract_address,
        contract_address  as project_contract_address,
        call_tx_hash as tx_hash,
        call_block_number as block_number,
        cast(null as uint256) as platform_fee_amount_raw,
        cast(null as uint256) as royalty_fee_amount_raw,
        cast(null as varbinary) as platform_fee_address,
        cast(null as varbinary) as royalty_fee_address,
        row_number() over (partition by call_tx_hash order by call_trace_address asc) as sub_tx_trade_id
    from (
        select *,
            case
                when orderType = '1' then json_extract_scalar(_order, '$.bid[0]')
                else json_extract_scalar(_order, '$.ask[0]')
            end as nft,
            case
                when orderType = '1' then json_extract_scalar(_order, '$.ask[0]')
                else json_extract_scalar(_order, '$.bid[0]')
            end as currency
        from (
            select *,
                json_extract_scalar(_order, '$.orderType') as orderType,
                cast(json_extract_scalar(_order, '$.amount') as uint256) as amount
            from (
                select t.*, _order
                from {{ source('liquidifty_ethereum', 'MarketplaceV3_call_buy') }} t
                cross join unnest(orders) as foo(_order)
                {% if is_incremental() %}
                where {{incremental_predicate('call_block_time')}}
                {% endif %}
                union all
                select t.*, _order
                from {{ source('liquidifty_ethereum', 'MarketplaceV3_deprecated_call_buy') }} t
                cross join unnest(orders) as foo(_order)
                {% if is_incremental() %}
                where {{incremental_predicate('call_block_time')}}
                {% endif %})
            where call_success
        )
    )
)

select
    *
from (
    select * from v2
    union all
    select * from stack
    union all
    select * from v3
)
