{{ config(
        alias ='flows',
        post_hook='{{ expose_spells(\'["ethereum", "optimism","polygon"]\',
                                "project",
                                "celer",
                                \'["soispoke"]\') }}'
        )
}}

{% set celer_models = [
'celer_v2_ethereum_flows',
'celer_v2_optimism_flows',
'celer_v2_polygon_flows'
] %}


SELECT *
FROM (
    {% for celer_model in celer_models %}
    SELECT
        blockchain,
        project,      
        version,
        contract_address,
        block_time,
        block_date,
        block_number,
        tx_hash,
        evt_index,
        tx_type,
        sender,
        receiver,
        token_address,
        token_symbol,
        token_amount_raw,
        token_amount,
        token_amount_usd,
        token_amount_native,
        transfer_id
    FROM {{ ref(celer_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;