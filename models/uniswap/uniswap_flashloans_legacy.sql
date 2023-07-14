{{ config(
	tags=['legacy'],
	
        alias = alias('flashloans', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon", "bnb"]\',
                                "project",
                                "uniswap",
                                \'["hildobby"]\') }}'
        )
}}

{% set uniswap_models = [
ref('uniswap_v3_ethereum_flashloans_legacy')
, ref('uniswap_v3_optimism_flashloans_legacy')
, ref('uniswap_v3_arbitrum_flashloans_legacy')
, ref('uniswap_v3_polygon_flashloans_legacy')
, ref('uniswap_v3_bnb_flashloans_legacy')
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