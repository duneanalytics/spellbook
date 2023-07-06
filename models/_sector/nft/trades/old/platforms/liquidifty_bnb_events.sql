{{ config(
    schema = 'liquidifty_bnb',
    alias = 'events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'unique_trade_id']
)}}

{% set wbnb_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' %}

with v1 as (
    select
        'v1' as version,
        evt_block_time as block_time,
        token as token_id,
        'erc721' as token_standard,
        'Single Item Trade' as trade_type,
        CAST(amount as decimal(38,0)) as number_of_items,
        'Buy' as trade_category,
        owner as seller,
        buyer,
        price as amount_raw,
        '{{ wbnb_address }}' as currency_contract,
        collection as nft_contract_address,
        contract_address  as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        evt_index as in_tx_id,
        'native' as currency_token_standard,
        '1' as orderType
    from {{ source('liquidifty_bnb', 'MarketplaceV1_evt_Buy') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),
v2 as (
    select
        'v2' as version,
        evt_block_time as block_time,
        token as token_id,
        'erc721' as token_standard,
        'Single Item Trade' as trade_type,
        CAST(amount as decimal(38,0)) as number_of_items,
        'Buy' as trade_category,
        owner as seller,
        buyer,
        price as amount_raw,
        case
            when currency = '0x0000000000000000000000000000000000000000' then '{{ wbnb_address }}'
            else currency
        end as currency_contract,
        collection as nft_contract_address,
        contract_address  as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        evt_index as in_tx_id,
        case
            when currency = '0x0000000000000000000000000000000000000000' then 'native'
            else 'erc20'
        end as currency_token_standard,
        '1' as orderType
    from (
        select *
        from {{ source('liquidifty_bnb', 'MarketplaceV2_evt_Buy') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        union all
        select *
        from {{ source('liquidifty_bnb', 'MarketplaceV2_1_evt_Buy') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        union all
        select *
        from {{ source('liquidifty_bnb', 'MarketplaceV2_5_evt_Buy') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ) a
),
stack as (
    select
        cast(null as varchar(5)) as version,
        evt_block_time as block_time,
        cast(null as varchar(5)) as token_id,
        'erc721' as token_standard,
        'Bundle Trade' as trade_type,
        CAST(amount as decimal(38,0)) as number_of_items,
        'Buy' as trade_category,
        owner as seller,
        buyer,
        price as amount_raw,
        case
            when currency = '0x0000000000000000000000000000000000000000' then '{{ wbnb_address }}'
            else currency
        end as currency_contract,
        collection as nft_contract_address,
        contract_address  as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        evt_index as in_tx_id,
        case
            when currency = '0x0000000000000000000000000000000000000000' then 'native'
            else 'erc20'
        end as currency_token_standard,
        '1' as orderType
    from {{ source('liquidifty_bnb', 'MultipleSell_evt_Buy') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),
v3 as (
    select
        'v3' as version,
        call_block_time as block_time,
        get_json_object(nft, '$.id') as token_id,
        case
            when get_json_object(nft, '$.assetType') = '0' then 'native'
            when get_json_object(nft, '$.assetType') = '1' then 'erc20'
            when get_json_object(nft, '$.assetType') = '2' then 'erc721'
            when get_json_object(nft, '$.assetType') = '3' then 'erc1155'
        end as token_standard,
        case
            when cardinality(orders) > 1 then 'Bundle Trade'
            else 'Single Item Trade'
        end as trade_type,
        CAST(get_json_object(nft, '$.amount') as decimal(38,0)) as number_of_items,
        case
            when orderType = '0' then 'Swap'
            when orderType = '1' then 'Buy'
            when orderType = '2' then 'Sell'
        end as trade_category,
        get_json_object(order, '$.signer') as seller,
        cast(null as varchar(5)) as buyer,
        get_json_object(currency, '$.amount') as amount_raw,
        case
            when get_json_object(currency, '$.assetType') = '0' then '{{ wbnb_address }}'
            else get_json_object(currency, '$.collection')
        end as currency_contract,
        get_json_object(nft, '$.collection') as nft_contract_address,
        contract_address  as project_contract_address,
        call_tx_hash as tx_hash,
        call_block_number as block_number,
        row_number() over (partition by call_tx_hash order by call_trace_address asc) as in_tx_id,
        case
            when get_json_object(currency, '$.assetType') = '0' then 'native'
            when get_json_object(currency, '$.assetType') = '1' then 'erc20'
            when get_json_object(currency, '$.assetType') = '2' then 'erc721'
            when get_json_object(currency, '$.assetType') = '3' then 'erc1155'
        end as currency_token_standard,
        orderType
    from (
        select *,
            case
                when orderType = '1' then get_json_object(order, '$.bid[0]')
                else get_json_object(order, '$.ask[0]')
            end as nft,
            case
                when orderType = '1' then get_json_object(order, '$.ask[0]')
                else get_json_object(order, '$.bid[0]')
            end as currency
        from (
            select *,
                get_json_object(order, '$.orderType') as orderType,
                get_json_object(order, '$.amount') as amount
            from (
                select *, explode(orders) as order
                from {{ source('liquidifty_bnb', 'MarketplaceV3_call_buy') }}
                {% if is_incremental() %}
                where call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
                union all
                select *, explode(orders) as order
                from {{ source('liquidifty_bnb', 'MarketplaceV3_deprecated_call_buy') }}
                {% if is_incremental() %}
                where call_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %})
            where call_success
        )
    )
)


select
    'bnb' as blockchain,
    'liquidifty' as project,
    buys.version,
    buys.block_time,
    buys.token_id,
    nft_tokens.name as collection,
    cast(buys.amount_raw as decimal(38, 0)) / power(10, bep20.decimals) * prices.price as amount_usd,
    buys.token_standard as token_standard,
    buys.trade_type,
    buys.number_of_items,
    buys.trade_category,
    'Trade' as evt_type,
    buys.seller,
    case
        when buys.version = 'v3' then transactions.from
        else buys.buyer
    end as buyer,
    cast(buys.amount_raw as decimal(38, 0)) / power(10, bep20.decimals) as amount_original,
    cast(buys.amount_raw as decimal(38, 0)) as amount_raw,
    bep20.symbol as currency_symbol,
    buys.currency_contract,
    buys.nft_contract_address,
    buys.project_contract_address,
    cast(null as varchar(5)) as aggregator_name,
    cast(null as varchar(5)) as aggregator_address,
    buys.tx_hash,
    buys.block_number,
    transactions.from as tx_from,
    transactions.to as tx_to,
    cast(null as decimal(38)) as platform_fee_amount_raw,
    cast(null as double) as platform_fee_amount,
    cast(null as double) as platform_fee_amount_usd,
    cast(null as double) as platform_fee_percentage,
    cast(null as decimal(38)) as royalty_fee_amount_raw,
    cast(null as double) as royalty_fee_amount,
    cast(null as double) as royalty_fee_amount_usd,
    cast(null as double) as royalty_fee_percentage,
    cast(null as varchar(1)) as royalty_fee_receive_address,
    bep20.symbol as royalty_fee_currency_symbol,
    concat(cast(buys.block_number as varchar(5)), '-',buys.tx_hash,'-', cast(in_tx_id as varchar(5))) as unique_trade_id,
    buys.currency_token_standard,
    buys.orderType as order_type
from (
    select * from v1
    union all
    select * from v2
    union all
    select * from stack
    union all
    select * from v3
) buys
left join {{ ref('tokens_bnb_bep20_legacy') }} bep20
    on bep20.contract_address = buys.currency_contract
left join {{ ref('tokens_bnb_nft_legacy') }} nft_tokens
    on nft_tokens.contract_address = buys.nft_contract_address
left join {{ source('prices', 'usd') }} as prices
    on prices.minute = date_trunc('minute', buys.block_time)
    and prices.contract_address = buys.currency_contract
    and prices.blockchain = 'bnb'
    {% if is_incremental() %}
    and prices.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
inner join {{ source('bnb', 'transactions') }} transactions
    on transactions.block_number = buys.block_number
    and transactions.hash = buys.tx_hash
    {% if is_incremental() %}
    and transactions.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
