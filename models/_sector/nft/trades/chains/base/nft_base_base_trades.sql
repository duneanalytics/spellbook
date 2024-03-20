{{ config(
    schema = 'nft_base',
    alias = 'base_trades',
    materialized = 'view'
    )
}}


{% set nft_models = [
 ref('alienswap_base_base_trades')
 ,ref('element_base_base_trades')
 ,ref('zonic_base_base_trades')
 ,ref('sudoswap_v2_base_base_trades')
] %}

with base_union as (
SELECT * FROM  (
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
    )
)
select * from base_union
