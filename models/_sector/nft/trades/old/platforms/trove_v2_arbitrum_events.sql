{{ config(
    schema='trove_v2_arbitrum',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_trade_id']
)}}

{% set project_start_date = '2022-03-12' %}

with marketplace as (
    select
        evt_block_time as block_time,
        tokenId as token_id,
        case
            when quantity= uint256 '1' then 'Single Item Trade'
            else 'Bulk Purchase'
        end as trade_type,
        quantity as number_of_items,
        'Buy' as trade_category,
        seller,
        buyer,
        cast(pricePerItem * quantity as uint256) as amount_raw,
        case
            when paymentToken is null then 0x539bde0d7dbd336b79148aa742883198bbf60342
            else paymentToken
        end as currency_contract,
        nftAddress as nft_contract_address,
        contract_address as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        evt_index
    from (
        select evt_block_time,
               evt_index,
               tokenId,
               quantity,
               seller,
               pricePerItem,
               paymentToken,
               nftAddress,
               evt_tx_hash,
               evt_block_number,
               contract_address,
               bidder as buyer
        from {{ source('treasure_trove_arbitrum', 'TreasureMarketplaceV2_evt_BidAccepted') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        where evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        union all
        select evt_block_time,
               evt_index,
               tokenId,
               quantity,
               seller,
               pricePerItem,
               paymentToken,
               nftAddress,
               evt_tx_hash,
               evt_block_number,
               contract_address,
               buyer
        from {{ source('treasure_trove_arbitrum', 'TreasureMarketplaceV2_evt_ItemSold') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        where evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
    )
)


select
    'arbitrum' as blockchain,
    'trove' as project,
    'v2' as version,
    date_trunc('day',mp.block_time) as block_date,
    mp.block_time,
    token_id,
    nft_tokens.name as collection,
    cast(prices.price * amount_raw / power(10, erc20.decimals) as double) as amount_usd,
    nft_tokens.standard as token_standard,
    trade_type,
    number_of_items,
    trade_category,
    'Trade' as evt_type,
    seller,
    buyer,
    cast(amount_raw / power(10, erc20.decimals) as double) as amount_original,
    amount_raw,
    erc20.symbol as currency_symbol,
    currency_contract,
    nft_contract_address,
    project_contract_address,
    cast(null as varchar) as aggregator_name,
    cast(null as varbinary) as aggregator_address,
    mp.tx_hash,
    mp.block_number,
    tx."from" as tx_from,
    tx.to as tx_to,
    cast(null as uint256) as platform_fee_amount_raw,
    cast(null as double) as platform_fee_amount,
    cast(null as double) as platform_fee_amount_usd,
    cast(null as double) as platform_fee_percentage,
    cast(null as uint256) as royalty_fee_amount_raw,
    cast(null as double) as royalty_fee_amount,
    cast(null as double) as royalty_fee_amount_usd,
    cast(null as double) as royalty_fee_percentage,
    cast(null as varbinary) as royalty_fee_receive_address,
    erc20.symbol as royalty_fee_currency_symbol,
    cast(mp.block_number as varchar) || '-' || cast(mp.tx_hash as varchar)|| '-' || cast(mp.evt_index as varchar) as unique_trade_id
from marketplace mp
inner join {{ source('arbitrum', 'transactions') }} tx
    on tx.block_number = mp.block_number
    and tx.hash = mp.tx_hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    and tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
left join {{ ref('tokens_arbitrum_erc20') }} erc20
    on erc20.contract_address = mp.currency_contract
left join {{ ref('tokens_arbitrum_nft') }} nft_tokens
    on nft_tokens.contract_address = mp.nft_contract_address
left join {{ source('prices', 'usd') }} as prices
    on prices.minute = date_trunc('minute', mp.block_time)
    and prices.contract_address = mp.currency_contract
    and prices.blockchain = 'arbitrum'
    {% if is_incremental() %}
    and prices.minute >= date_trunc('day', now() - interval '7' day)
    {% else %}
    and prices.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
