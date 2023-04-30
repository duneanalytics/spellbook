{{ config(
        alias ='events'
)
}}

select 
    blockchain,
    project,
    version,
    block_time,
    token_id,
    collection,
    amount_usd,
    token_standard,
    trade_type,
    CAST(number_of_items AS DECIMAL(38,0)) number_of_items,
    trade_category,
    evt_type,
    seller,
    buyer,
    amount_original,
    CAST(amount_raw AS DECIMAL(38,0)) amount_raw,
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
    CAST(platform_fee_amount / amount_original AS DOUBLE) platform_fee_percentage,
    royalty_fee_amount_raw,
    royalty_fee_amount,
    royalty_fee_amount_usd,
    CAST(royalty_fee_amount / amount_original AS DOUBLE) royalty_fee_percentage,
    royalty_fee_receive_address,
    currency_symbol as royalty_fee_currency_symbol,
    unique_trade_id
from {{ ref('seaport_optimism_trades') }}
where 
    platform_fee_receive_address = '0x0000a26b00c1f0df003000390027140000faa719'
