{% set blockchain = 'base' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'withdrawals',
    materialized = 'view'
    )
}}

{% set bridge_platforms = [
    'bridge_base_cctp_v1_withdrawals'
] %}

SELECT *
FROM (
    {% for bridge_platform in bridge_platforms %}
    SELECT deposit_chain
    , withdrawal_chain
    , project
    , project_version
    , intent_based
    , canonical_bridge
    , block_date
    , block_time
    , block_number
    , withdrawal_amount_raw
    , sender
    , recipient
    , withdrawal_token_standard
    , withdrawal_token_address
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