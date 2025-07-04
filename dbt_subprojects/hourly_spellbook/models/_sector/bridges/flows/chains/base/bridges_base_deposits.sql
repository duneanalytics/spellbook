{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'deposits',
    materialized = 'view'
    )
}}

{% set bridges_platforms = [
    'bridges_base_cctp_v1_deposits'
] %}

SELECT *
FROM (
    {% for bridge_platform in bridges_platforms %}
    SELECT deposit_chain
    , withdrawal_chain
    , project
    , project_version
    , intent_based
    , canonical_bridge
    , block_date
    , block_time
    , block_number
    , deposit_amount_raw
    , sender
    , recipient
    , deposit_token_standard
    , withdrawal_token_standard
    , deposit_token_address
    , tx_from
    , tx_hash
    , evt_index
    , contract_address
    , transfer_id
    FROM {{ ref(bridge_platform) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)