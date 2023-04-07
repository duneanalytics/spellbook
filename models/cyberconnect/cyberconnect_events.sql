{{ config(
        alias ='cyberconnect_events',
        post_hook='{{ expose_spells(\'["optimism","polygon","bnb"]\',
                                "project",
                                "cyberconnect",
                                \'["msilb7","0xroll"]\') }}'
        )
}}

{% set cyberconnect_models = [
'cyberconnect_bnb_events'
,'cyberconnect_ethereum_events'

] %}

SELECT *
FROM (
    {% for model in cyberconnect_models %}
    SELECT
        blockchain,
        project,
        action,
        name,
        contract_address,
        evt_block_time,
        block_date,
        evt_block_number,
        evt_tx_hash,
        evt_type,
        buyer,
        seller,
        handle,
        profile_id,
        content_id,
        content_uri,
        unique_event_id
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;