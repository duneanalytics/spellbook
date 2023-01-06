{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum","bnb","avalanche_c"]\',
                                            "project",
                                            "element",
                                            \'["hildobby","soispoke"]\') }}'
)
}}

{% set element_models = [
'element_ethereum_trades'
,'element_avalanche_c_trades'
,'element_bnb_trades'
] %}


SELECT *
FROM (
    {% for model in element_models %}
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
        block_number,
        tx_hash,
        tx_from,
        tx_to,
        unique_trade_id
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;