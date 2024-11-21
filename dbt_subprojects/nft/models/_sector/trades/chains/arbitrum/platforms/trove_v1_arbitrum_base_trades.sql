{{ config(
    schema='trove_v1_arbitrum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
)}}

{% set project_start_date = '2021-11-13' %}

with base_trades as (
    select
        'arbitrum' as blockchain,
        'trove' as project,
        'v1' as project_version,
        evt_block_time as block_time,
        date_trunc('day',evt_block_time) as block_date,
        date_trunc('month',evt_block_time) as block_month,
        tokenId as nft_token_id,
        'secondary' as trade_type,
        quantity as nft_amount,
        'Buy' as trade_category,
        seller,
        buyer,
        cast(pricePerItem * quantity as uint256) as price_raw,
        0x539bde0d7dbd336b79148aa742883198bbf60342 as currency_contract,
        nftAddress as nft_contract_address,
        contract_address as project_contract_address,
        evt_tx_hash as tx_hash,
        evt_block_number as block_number,
        cast(null as uint256) as platform_fee_amount_raw,
        cast(null as uint256) as royalty_fee_amount_raw,
        cast(null as varbinary) as royalty_fee_address,
        cast(null as varbinary) as platform_fee_address,
        evt_index as sub_tx_trade_id
    from {{ source('treasure_trove_arbitrum', 'TreasureMarketplaceV1_evt_ItemSold') }}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% else %}
    where evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

{{add_nft_tx_data('base_trades','arbitrum')}}
