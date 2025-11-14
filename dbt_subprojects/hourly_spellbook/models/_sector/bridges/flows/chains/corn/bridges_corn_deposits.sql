{% set blockchain = 'corn' %}

{{ config(
    schema = 'bridges_' + blockchain
    , alias = 'deposits'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['deposit_chain', 'bridge_name', 'bridge_version', 'withdrawal_chain_id', 'tx_hash', 'evt_index', 'bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set bridges_platforms = [
'bridges_' + blockchain + '_tether_v1_deposits'
] %}

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
FROM (
    SELECT *
        , ROW_NUMBER() OVER (PARTITION BY deposit_chain, tx_hash, evt_index, bridge_transfer_id ORDER BY block_number, block_time) AS rn
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
