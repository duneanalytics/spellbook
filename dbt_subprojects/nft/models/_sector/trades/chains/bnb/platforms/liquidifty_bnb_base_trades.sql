{{ config(
    schema = 'liquidifty_bnb',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
)}}

{% set wbnb_address = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c" %}

with v1 as (
    select
        'v1' as version,
        evt_block_time as block_time,
        cast(token as uint256) as token_id,
        'erc721' as token_standard,
        'secondary' as trade_type,
        CAST(amount as uint256) as number_of_items,
        'Buy' as trade_category,
        owner as seller,
        buyer,
        price as amount_raw,
        {{ wbnb_address }} as currency_contract,
        collection as nft_contract_address,
        contract_address  as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        evt_index as in_tx_id,
        'native' as currency_token_standard,
        '1' as orderType
    from {{ source('liquidifty_bnb', 'MarketplaceV1_evt_Buy') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),
v2 as (
    select
        'v2' as version,
        evt_block_time as block_time,
        cast(token as uint256) as token_id,
        'erc721' as token_standard,
        'secondary' as trade_type,
        CAST(amount as uint256) as number_of_items,
        'Buy' as trade_category,
        owner as seller,
        buyer,
        price as amount_raw,
        case
            when currency = 0x0000000000000000000000000000000000000000 then {{ wbnb_address }}
            else currency
        end as currency_contract,
        collection as nft_contract_address,
        contract_address  as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        evt_index as in_tx_id,
        case
            when currency = 0x0000000000000000000000000000000000000000 then 'native'
            else 'erc20'
        end as currency_token_standard,
        '1' as orderType
    from (
        select *
        from {{ source('liquidifty_bnb', 'MarketplaceV2_evt_Buy') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        union all
        select *
        from {{ source('liquidifty_bnb', 'MarketplaceV2_1_evt_Buy') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        union all
        select *
        from {{ source('liquidifty_bnb', 'MarketplaceV2_5_evt_Buy') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    ) a
),
stack as (
    select
        'v2' as version,
        evt_block_time as block_time,
        cast(null as uint256) as token_id,
        'erc721' as token_standard,
        'secondary' as trade_type,
        CAST(amount as uint256) as number_of_items,
        'Buy' as trade_category,
        owner as seller,
        buyer,
        price as amount_raw,
        case
            when currency = 0x0000000000000000000000000000000000000000 then {{ wbnb_address }}
            else currency
        end as currency_contract,
        collection as nft_contract_address,
        contract_address  as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        evt_index as in_tx_id,
        case
            when currency = 0x0000000000000000000000000000000000000000 then 'native'
            else 'erc20'
        end as currency_token_standard,
        '1' as orderType
    from {{ source('liquidifty_bnb', 'MultipleSell_evt_Buy') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),
v3 as (
    select
        'v3' as version,
        call_block_time as block_time,
        cast(json_extract_scalar(nft, '$.id') as uint256) as token_id,
        case
            when json_extract_scalar(nft, '$.assetType') = '0' then 'native'
            when json_extract_scalar(nft, '$.assetType') = '1' then 'erc20'
            when json_extract_scalar(nft, '$.assetType') = '2' then 'erc721'
            when json_extract_scalar(nft, '$.assetType') = '3' then 'erc1155'
        end as token_standard,
        'secondary' as trade_type,
        CAST(JSON_EXTRACT_SCALAR(nft, '$.amount') as uint256) as number_of_items,
        case
            when orderType = '0' then 'Swap'
            when orderType = '1' then 'Buy'
            when orderType = '2' then 'Sell'
        end as trade_category,
        from_hex(json_extract_scalar(_order, '$.signer')) as seller,
        cast(null as varbinary) as buyer,
        cast(json_extract_scalar(currency, '$.amount') as uint256) as amount_raw,
        case
            when json_extract_scalar(currency, '$.assetType') = '0' then {{ wbnb_address }}
            else cast(json_extract_scalar(currency, '$.collection') as varbinary)
        end as currency_contract,
        from_hex(json_extract_scalar(nft, '$.collection')) as nft_contract_address,
        contract_address  as project_contract_address,
        call_tx_hash as tx_hash,
        call_block_number as block_number,
        row_number() over (partition by call_tx_hash order by call_trace_address asc) as in_tx_id,
        case
            when json_extract_scalar(currency, '$.assetType') = '0' then 'native'
            when json_extract_scalar(currency, '$.assetType') = '1' then 'erc20'
            when json_extract_scalar(currency, '$.assetType') = '2' then 'erc721'
            when json_extract_scalar(currency, '$.assetType') = '3' then 'erc1155'
        end as currency_token_standard,
        orderType
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
                from {{ source('liquidifty_bnb', 'MarketplaceV3_call_buy') }} t
                cross join unnest(orders) as foo(_order)
                {% if is_incremental() %}
                where call_block_time >= date_trunc('day', now() - interval '7' day)
                {% endif %}
                union all
                select t.*, _order
                from {{ source('liquidifty_bnb', 'MarketplaceV3_deprecated_call_buy') }} t
                cross join unnest(orders) as foo(_order)
                {% if is_incremental() %}
                where call_block_time >= date_trunc('day', now() - interval '7' day)
                {% endif %})
            where call_success
        )
    )
)

,base_trades as (
select
    'bnb' as blockchain,
    'liquidifty' as project,
    buys.version as project_version,
    buys.block_time,
    cast(date_trunc('day', buys.block_time) as date) as block_date,
    cast(date_trunc('month', buys.block_time) as date) as block_month,
    buys.token_id as nft_token_id,
    buys.trade_type,
    buys.number_of_items as nft_amount,
    buys.trade_category,
    buys.seller,
    buys.buyer,
    buys.amount_raw as price_raw,
    buys.currency_contract,
    buys.nft_contract_address,
    buys.project_contract_address,
    buys.tx_hash,
    buys.block_number,
    cast(null as uint256) as platform_fee_amount_raw,
    cast(null as uint256) as royalty_fee_amount_raw,
    cast(null as varbinary) as platform_fee_address,
    cast(null as varbinary) as royalty_fee_address,
    buys.in_tx_id as sub_tx_trade_id
from (
    select * from v1
    union all
    select * from v2
    union all
    select * from stack
    union all
    select * from v3
    ) buys
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'bnb') }}
