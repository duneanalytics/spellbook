{% set blockchain = 'base' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'withdrawals',
    materialized = 'view'
    )
}}

{% set bridge_platforms = [
    'bridge_base_circle_withdrawals'
] %}

SELECT *
FROM (
    {% for bridge_platform in bridge_platforms %}
    SELECT deposit_chain
    , withdraw_chain
    , project
    , project_version
    , block_date
    , block_time
    , block_number
    , deposit_amount_raw
    , withdraw_amount_raw
    , sender
    , recipient
    , deposit_token_standard
    , withdraw_token_standard
    , deposit_token_address
    , withdraw_token_address
    , tx_from
    , tx_hash
    , evt_index
    , contract_address
    , bridge_id
    FROM {{ ref(bridge_platform) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)