{{ config(
        alias ='fees',
        post_hook='{{ expose_spells(\'["arbitrum","bnb"]\',
                                    "project",
                                    "tofu",
                                    \'["Henrystats", "theachenyj", "chuxin"]\') }}')
}}


{% set tofu_models = [
 ref('tofu_bnb_fees')
,ref('tofu_arbitrum_fees')
,ref('tofu_optimism_fees')
] %}

SELECT *
FROM (
    {% for nft_model in tofu_models %}
    SELECT
        blockchain,
        project,
        version,
        block_time,
        token_id,
        collection,
        platform_fee_amount_raw,
        platform_fee_amount,
        platform_fee_amount_usd,
        platform_fee_percentage,
        royalty_fee_amount_raw,
        royalty_fee_amount,
        royalty_fee_amount_usd,
        royalty_fee_percentage,
        royalty_fee_receive_address,
        royalty_fee_currency_symbol,
        token_standard,
        trade_type,
        number_of_items,
        trade_category,
        evt_type,
        seller,
        buyer,
        nft_contract_address,
        project_contract_address,
        aggregator_name,
        aggregator_address,
        tx_hash,
        block_number,
        tx_from,
        tx_to,
        unique_trade_id
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)