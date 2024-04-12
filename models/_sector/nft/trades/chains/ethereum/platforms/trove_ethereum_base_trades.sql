{{ config(
    schema='trove_ethereum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
)}}

{% set project_start_date = '2022-08-16' %}

with all_trades as (
    select evt_block_time,
           evt_index,
           tokenId,
           quantity,
           seller,
           'Sell' as trade_category,
           pricePerItem,
           paymentToken,
           nftAddress,
           evt_tx_hash,
           evt_block_number,
           contract_address,
           bidder as buyer
    from {{ source('treasure_trove_ethereum', 'TreasureMarketplace_evt_BidAccepted') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% else %}
    where evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    union all
    select evt_block_time,
           evt_index,
           tokenId,
           quantity,
           seller,
           'Buy' as trade_category,
           pricePerItem,
           paymentToken,
           nftAddress,
           evt_tx_hash,
           evt_block_number,
           contract_address,
           buyer
    from {{ source('treasure_trove_ethereum', 'TreasureMarketplace_evt_ItemSold') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% else %}
    where evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

select
    'ethereum' as blockchain,
    'trove' as project,
    'v2' as project_version,
    evt_block_time as block_time,
    date_trunc('day',evt_block_time) as block_date,
    date_trunc('month',evt_block_time) as block_month,
    tokenId as nft_token_id,
    'secondary' as trade_type,
    quantity as nft_amount,
    trade_category,
    seller,
    buyer,
    cast(pricePerItem * quantity as uint256) as price_raw,
    paymentToken as currency_contract,
    nftAddress as nft_contract_address,
    contract_address as project_contract_address,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    cast(null as uint256) as platform_fee_amount_raw,
    cast(null as uint256) as royalty_fee_amount_raw,
    cast(null as varbinary) as royalty_fee_address,
    cast(null as varbinary) as platform_fee_address,
    evt_index as sub_tx_trade_id
from all_trades
