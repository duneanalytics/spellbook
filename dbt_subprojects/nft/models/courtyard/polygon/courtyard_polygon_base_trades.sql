{{ config(
    schema = 'courtyard_polygon',
    alias = 'base_trades',
    materialized = 'view'
    )
}}

SELECT
    blockchain,
    project,
    version as project_version,
    block_time,
    block_date,
    block_month,
    block_number,
    tx_hash,
    '' as project_contract_address,
    trade_type as trade_category,
    'Trade' as trade_type,
    buyer,
    seller,
    nft_contract_address,
    token_id as nft_token_id,
    token_amount as nft_amount,
    price as price_raw,
    payment_token as currency_contract,
    platform_fee_usd as platform_fee_amount_raw,
    royalty_fee_usd as royalty_fee_amount_raw,
    '' as platform_fee_address,
    '' as royalty_fee_address,
    sub_idx as sub_tx_trade_id,
    tx_hash as tx_from,
    tx_hash as tx_to,
    '' as tx_data_marker
FROM {{ ref('courtyard_polygon_trades') }}