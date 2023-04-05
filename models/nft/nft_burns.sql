{{ config(
        alias ='burns',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum","solana","bnb"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke","0xRob"]\') }}')
}}


{% set nft_models = [
 ref('blur_ethereum_burns')
,ref('element_burns')
,ref('foundation_ethereum_burns')
,ref('looksrare_ethereum_burns')
,ref('opensea_burns')
,ref('x2y2_ethereum_burns')
,ref('zora_ethereum_burns')
,ref('nftb_bnb_burns')
] %}


SELECT *
FROM (
    {% for nft_model in nft_models %}
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
