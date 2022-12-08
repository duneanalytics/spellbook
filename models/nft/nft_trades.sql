{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum","solana"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke"]\') }}')
}}

{% set nft_models = [
'opensea_trades'
,'magiceden_trades'
,'looksrare_ethereum_trades'
,'x2y2_ethereum_trades'
,'sudoswap_ethereum_trades'
,'foundation_ethereum_trades'
,'archipelago_ethereum_trades'
,'cryptopunks_ethereum_trades'
,'element_trades'
,'superrare_ethereum_trades'
,'zora_ethereum_trades'
,'blur_ethereum_trades'
] %}


SELECT *
FROM (
    {% for model in nft_models %}
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
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
