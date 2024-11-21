{{
    config(
        schema = 'tevaera_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{% set tevaera_usage_start_date = "2023-07-26" %}

with base_trades as (
    select
        'zksync' as blockchain,
        'tevaera' as project,
        '1' as project_version,
        s.evt_block_time as block_time,
        cast(date_trunc('day', s.evt_block_time) as date) as block_date,
        cast(date_trunc('month', s.evt_block_time) as date) as block_month,
        s.evt_block_number as block_number,
        s.assetContract as nft_contract_address,
        s.tokenId as nft_token_id,
        s.quantityBought as nft_amount,
        s.lister as seller,
        s.buyer as buyer,
        'Buy' as trade_category,
        'secondary' as trade_type,
        s.totalPricePaid as price_raw,
        0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91 as currency_contract, -- WETH
        s.contract_address as project_contract_address,
        s.evt_tx_hash as tx_hash,
        cast(null as uint256) as platform_fee_amount_raw,
        cast(null as uint256) as royalty_fee_amount_raw,
        cast(null as varbinary) as royalty_fee_address,
        cast(null as varbinary) as platform_fee_address,
        s.evt_index as sub_tx_trade_id
    from {{ source('tevaera_zksync', 'TevaMarket_evt_NewSale') }} s
    {% if is_incremental() %}
    where {{incremental_predicate('s.evt_block_time')}}
    {% else %}
    where s.evt_block_time >= timestamp '{{tevaera_usage_start_date}}'
    {% endif %}
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'zksync') }}
