{{ config(
    schema = 'blur_v2_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set blur_v2_start_date = '2023-07-05' %}

SELECT 'zora' as blockchain
, 'zora' as project
, 'v1' as project_version
, block_time
, block_number
, tx_hash
, sub_tx_trade_id
, CASE WHEN bt.order_type = 1 THEN 'Sell' ELSE 'Buy' END AS trade_category
, 'secondary' AS trade_type
, buyer
, seller
, nft_contract_address
, nft_token_id
, nft_amount
, price_raw
, currency_contract
, project_contract_address
, platform_fee_amount_raw
, platform_fee_address
, royalty_fee_amount_raw
, bt.royalty_fee_address
FROM {{ source('blur_v2_ethereum','BlurPool_evt_Execution721TakerFeePacked') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% else %}
WHERE evt_block_time >= TIMESTAMP '{{blur_v2_start_date}}'
{% endif %}