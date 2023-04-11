{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["arbitrum","bnb", "optimism"]\',
                                    "project",
                                    "tofu",
                                    \'["Henrystats", "theachenyj", "chuxin"]\') }}')
}}


{% set tofu_models = [
 ref('tofu_bnb_trades')
,ref('tofu_arbitrum_trades')
,ref('tofu_optimism_trades')
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
        amount_usd,
        token_standard,
        trade_type,
        number_of_items,
        trade_category,
        evt_type,
        seller,
        buyer,
        amount_original,
        amount_raw,
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
        unique_trade_id
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

)