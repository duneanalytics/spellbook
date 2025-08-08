{% set blockchain = 'unichain' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'deposits',
    materialized = 'view'
    )
}}

{% set bridges_platforms = [
    'bridges_' + blockchain + '_cctp_v1_deposits'
] %}

SELECT *
FROM (
    {% for bridge_platform in bridges_platforms %}
    SELECT deposit_chain
    , withdrawal_chain
    , bridge_name
    , bridge_version
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
    , bridge_transfer_id
    FROM {{ ref(bridge_platform) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)