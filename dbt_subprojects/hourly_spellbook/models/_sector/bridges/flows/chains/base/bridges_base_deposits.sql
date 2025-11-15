{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain
    , alias = 'deposits'
    , materialized = 'incremental'
    , file_format = 'delta'
    , partition_by = ['block_month']
    , incremental_strategy='merge'
    , unique_key = ['tx_hash', 'evt_index', 'bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
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
    , 'bridges_' + blockchain + '_layerzero_deposits'
    , 'bridges_' + blockchain + '_beamer_v2_deposits'
    , 'bridges_' + blockchain + '_symbiosis_v1_deposits'
    , 'bridges_' + blockchain + '_connext_v1_deposits'
    , 'bridges_' + blockchain + '_nitro_v1_deposits'
    , 'bridges_' + blockchain + '_beamer_v3_deposits'
] %}

SELECT deposit_chain
    , withdrawal_chain_id
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , cast(date_trunc('month', block_date) as date) as block_month
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
FROM (
    SELECT *
        , ROW_NUMBER() OVER (PARTITION BY block_date, block_time, block_number, deposit_chain, tx_hash, evt_index, bridge_transfer_id ORDER BY block_number, block_time) AS rn
    FROM (
        {% for bridge_platform in bridges_platforms %}
        SELECT d.deposit_chain
        , d.withdrawal_chain_id
        , d.withdrawal_chain
        , d.bridge_name
        , d.bridge_version
        , d.block_date
        , d.block_time
        , d.block_number
        , d.deposit_amount_raw
        , d.sender
        , d.recipient
        , d.deposit_token_standard
        , d.deposit_token_address
        , d.tx_from
        , d.tx_hash
        , d.evt_index
        , d.contract_address
        , d.bridge_transfer_id
        FROM {{ ref(bridge_platform) }} d
        {% if is_incremental() %}
        LEFT JOIN {{this}} t ON t.deposit_chain = '{{chain}}'
            AND d.block_date = t.block_date
            AND d.block_time = t.block_time
            AND d.block_number = t.block_number
            AND d.bridge_name = t.bridge_name
            AND d.bridge_version = t.bridge_version
            AND d.withdrawal_chain_id = t.withdrawal_chain_id
            AND d.tx_hash = t.tx_hash
            AND d.evt_index = t.evt_index
            AND d.bridge_transfer_id = t.bridge_transfer_id
        WHERE {{ incremental_predicate('d.block_time') }}
        AND t.block_time IS NULL
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )
    )
WHERE rn = 1
