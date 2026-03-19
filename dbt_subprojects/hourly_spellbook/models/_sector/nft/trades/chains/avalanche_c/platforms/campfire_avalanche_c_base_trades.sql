{{ config(
    schema='campfire_avalanche_c',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
)}}

{% set project_start_date = '2022-03-14' %}

WITH 

base_trades as (
SELECT 
    'avalanche_c' as blockchain,
    'campfire' as project,
    'v1' as project_version,
    evt_block_time as block_time,
    date_trunc('day',evt_block_time) as block_date,
    date_trunc('month',evt_block_time) as block_month,
    nftTokenId as nft_token_id,
    'secondary' as trade_type,
    UINT256 '1' as nft_amount,
    CASE 
        WHEN kind = UINT256 '1' THEN 'Buy'
        ELSE 'Sell'
    END as trade_category,
    seller,
    buyer,
    price as price_raw,
    0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7 as currency_contract,
    nftContractAddress as nft_contract_address,
    evt_tx_hash as tx_hash,
    contract_address as project_contract_address,
    evt_block_number as block_number,
    CAST(NULL as UINT256) as platform_fee_amount_raw,
    CAST(NULL as UINT256) as royalty_fee_amount_raw,
    CAST(NULL as VARBINARY) as royalty_fee_address,
    CAST(NULL as VARBINARY) as platform_fee_address,
    evt_index as sub_tx_trade_id
FROM 
{{ source('campfire_avalanche_c', 'CampfireMarket_evt_Sale') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% else %}
WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
{% endif %} 
)

{{add_nft_tx_data('base_trades','avalanche_c')}}
