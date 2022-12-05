{{ config(
        alias ='flows',
        post_hook='{{ expose_spells(\'["ethereum", "optimism"]\',
                                "sector",
                                "bridge",
                                \'["soispoke"]\') }}'
        )
}}

{% set bridge_models = [
'celer_v2_flows'
] %}

SELECT *
FROM (
    {% for bridge_model in bridge_models %}
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
    FROM {{ ref(bridge_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;