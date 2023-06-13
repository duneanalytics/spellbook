{{ config(
    schema = 'nft',
    alias ='trades_events_forward',
    materialized = 'view'
    )
}}


SELECT
    blockchain,
    project,
    version as project_version,
    block_date,
    block_time,
    token_id as nft_token_id,
    collection as nft_collection,
    amount_usd as price_usd,
    token_standard as nft_standard,
    'secondary' as trade_type,
    number_of_items as nft_amount,
    trade_category,
    seller,
    buyer,
    amount_original as price,
    amount_raw as price_raw,
    currency_symbol,
    currency_contract,
    nft_contract_address,
    project_contract_address,
    aggregator_name,
    aggregator_address,
    tx_hash,
    block_number,
    tx_from,
    tx_to,
    platform_fee_amount_raw,
    platform_fee_amount,
    platform_fee_amount_usd,
    platform_fee_percentage,
    royalty_fee_receive_address as royalty_fee_address,
    currency_symbol as royalty_fee_currency_symbol,
    royalty_fee_amount_raw,
    royalty_fee_amount,
    royalty_fee_amount_usd,
    royalty_fee_percentage,
    unique_trade_id
from {{ref('nft_events_old')}}
where  evt_type != 'Mint'
