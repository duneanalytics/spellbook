{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'deposits',
    materialized = 'view'
    )
}}

{% set bridges_platforms = [
    'bridges_' + blockchain + '_cctp_v1_deposits'
    , 'bridges_' + blockchain + '_celer_v1_deposits'
    , 'bridges_' + blockchain + '_across_v2_deposits'
    , 'bridges_' + blockchain + '_across_v3_deposits'
    , 'bridges_' + blockchain + '_butter_v2_deposits'
    , 'bridges_' + blockchain + '_axelar_gateway_deposits'
    , 'bridges_' + blockchain + '_synapse_rfq_deposits'
    , 'bridges_' + blockchain + '_zkbridge_v1_deposits'
    , 'bridges_' + blockchain + '_beamer_v2_deposits'
    , 'bridges_' + blockchain + '_symbiosis_v1_deposits'
    , 'bridges_' + blockchain + '_connext_v1_deposits'
    , 'bridges_' + blockchain + '_nitro_v1_deposits'
    , 'bridges_' + blockchain + '_beamer_v3_deposits'
    , 'bridges_' + blockchain + '_orbiter_v1_deposits'
    , 'bridges_' + blockchain + '_symbiosis_v1_deposits'
    , 'bridges_' + blockchain + '_connext_v1_deposits'
    , 'bridges_' + blockchain + '_nitro_v1_deposits'
    , 'bridges_' + blockchain + '_beamer_v3_deposits'
] %}

SELECT *
FROM (
    {% for bridge_platform in bridges_platforms %}
    SELECT deposit_chain
    , withdrawal_chain_id
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