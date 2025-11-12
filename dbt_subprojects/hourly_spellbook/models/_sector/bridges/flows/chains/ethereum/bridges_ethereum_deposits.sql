{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'deposits',
    materialized = 'view'
    )
}}

{% set bridges_platforms = [
    'bridges_' + blockchain + '_cctp_v1_deposits'
    , 'bridges_' + blockchain + '_celer_v1_deposits'
    , 'bridges_' + blockchain + '_across_v1_deposits'
    , 'bridges_' + blockchain + '_across_v2_deposits'
    , 'bridges_' + blockchain + '_across_v3_deposits'
    , 'bridges_' + blockchain + '_arbitrum_native_v1_deposits'
    , 'bridges_' + blockchain + '_ronin_native_v1_deposits'
    , 'bridges_' + blockchain + '_ronin_native_v2_deposits'
    , 'bridges_' + blockchain + '_lighter_v1_deposits'
    , 'bridges_' + blockchain + '_avalanche_c_native_v2_deposits'
    , 'bridges_' + blockchain + '_zkync_native_v2_deposits'
    , 'bridges_' + blockchain + '_blast_native_v1_deposits'
    , 'bridges_' + blockchain + '_sui_native_v1_deposits'
    , 'bridges_' + blockchain + '_synapse_rfq_deposits'
    , 'bridges_' + blockchain + '_zkbridge_v1_deposits'
    , 'bridges_' + blockchain + '_rainbow_v1_deposits'
    , 'bridges_' + blockchain + '_beamer_v1_deposits'
    , 'bridges_' + blockchain + '_beamer_v2_deposits'
    , 'bridges_' + blockchain + '_beamer_v3_deposits'
    , 'bridges_' + blockchain + '_polygon_native_v1_deposits'
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
    , deposit_token_address
    , deposit_token_standard
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
