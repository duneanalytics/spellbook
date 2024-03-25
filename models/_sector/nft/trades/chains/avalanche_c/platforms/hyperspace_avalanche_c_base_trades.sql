{{ config(
    schema='hyperspace_avalanche_c',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
)}}

{% set project_start_date = '2023-09-01' %}

WITH 

base_trades as (
SELECT 
    'avalanche_c' as blockchain,
    'hyperspace' as project,
    'v1' as project_version,
    evt_block_time as block_time,
    date_trunc('day',evt_block_time) as block_date,
    date_trunc('month',evt_block_time) as block_month,
    erc721TokenId as nft_token_id,
    'secondary' as trade_type,
    UINT256 '1' as nft_amount,
    CASE 
        WHEN direction = 0 THEN 'Buy'
        ELSE 'Sell'
    END as trade_category,
    CASE 
        WHEN direction = 0 THEN maker 
        ELSE taker
    END as seller, 
    CASE 
        WHEN direction = 0 THEN taker
        ELSE maker
    END as buyer,
    erc20TokenAmount as price_raw,
    0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7 as currency_contract,
    erc721Token as nft_contract_address,
    evt_tx_hash as tx_hash,
    contract_address as project_contract_address,
    evt_block_number as block_number,
    CAST(NULL as UINT256) as platform_fee_amount_raw,
    CAST(NULL as UINT256) as royalty_fee_amount_raw,
    CAST(NULL as VARBINARY) as royalty_fee_address,
    CAST(NULL as VARBINARY) as platform_fee_address,
    evt_index as sub_tx_trade_id
FROM 
{{ source('hyperspace_avalanche_c', 'ERC721OrdersFeature_evt_ERC721OrderFilled') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% else %}
WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
{% endif %} 
)

{{add_nft_tx_data('base_trades','avalanche_c')}}
