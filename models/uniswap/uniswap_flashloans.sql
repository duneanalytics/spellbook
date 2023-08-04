{{ config(
        alias = alias('flashloans'),
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon", "bnb"]\',
                                "project",
                                "uniswap",
                                \'["hildobby"]\') }}'
        )
}}

{% set uniswap_models = [
ref('uniswap_v3_ethereum_flashloans')
, ref('uniswap_v3_optimism_flashloans')
, ref('uniswap_v3_arbitrum_flashloans')
, ref('uniswap_v3_polygon_flashloans')
, ref('uniswap_v3_bnb_flashloans')
] %}


SELECT *
FROM (
    {% for flash_model in uniswap_models %}
    SELECT
        blockchain,
        project,
        version,
        block_time,
        amount,
        amount_usd,
        tx_hash,
        evt_index,
        fee,
        currency_contract,
        currency_symbol,
        contract_address
    FROM {{ flash_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;