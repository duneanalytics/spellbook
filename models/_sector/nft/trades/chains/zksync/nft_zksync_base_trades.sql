{{
    config(
        schema = 'nft_zksync',
        alias = 'base_trades',
        materialized = 'view'
    )
}}

{% set nft_models = [
    ref('zonic_zksync_base_trades'),
    ref('tevaera_zksync_base_trades'),
    ref('kreatorland_zksync_base_trades'),
    ref('element_zksync_base_trades'),
    ref('zk_markets_zksync_base_trades'),
    ref('mint_square_zksync_base_trades')
] %}

{% for nft_model in nft_models %}
SELECT
    blockchain,
    project,
    project_version,
    block_time,
    block_date,
    block_month,
    block_number,
    tx_hash,
    project_contract_address,
    trade_category,
    trade_type,
    buyer,
    seller,
    nft_contract_address,
    nft_token_id,
    nft_amount,
    price_raw,
    currency_contract,
    platform_fee_amount_raw,
    royalty_fee_amount_raw,
    platform_fee_address,
    royalty_fee_address,
    sub_tx_trade_id,
    tx_from,
    tx_to,
    tx_data_marker
FROM {{ nft_model }}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
