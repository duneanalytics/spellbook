{{ config(
        alias ='flashloans',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon", "gnosis"]\',
                                "project",
                                "balancer",
                                \'["hildobby"]\') }}'
        )
}}

{% set balancer_models = [
ref('balancer_ethereum_flashloans')
, ref('balancer_optimism_flashloans')
, ref('balancer_arbitrum_flashloans')
, ref('balancer_polygon_flashloans')
, ref('balancer_gnosis_flashloans')
] %}


SELECT *
FROM (
    {% for flash_model in balancer_models %}
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