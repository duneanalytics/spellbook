{{ config(
	tags=['legacy'],
	
        alias = alias('flashloans', legacy_model=True),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "optimism", "arbitrum", "fantom", "polygon"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{% set flashloan_models = [
ref('aave_flashloans_legacy')
, ref('balancer_flashloans_legacy')
, ref('dydx_ethereum_flashloans_legacy')
, ref('uniswap_flashloans_legacy')
, ref('euler_ethereum_flashloans_legacy')
, ref('maker_ethereum_flashloans_legacy')
, ref('synapse_flashloans_legacy')
, ref('fiat_dao_ethereum_flashloans_legacy')
, ref('equalizer_flashloans_legacy')
] %}


SELECT *
FROM (
    {% for flash_model in flashloan_models %}
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