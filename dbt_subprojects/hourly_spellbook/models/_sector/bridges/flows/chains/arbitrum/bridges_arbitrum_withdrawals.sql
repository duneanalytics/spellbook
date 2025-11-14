{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'bridges_' + blockchain
    , alias = 'withdrawals'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy='merge'
    , unique_key = ['withdrawal_chain', 'bridge_name', 'bridge_version', 'deposit_chain_id', 'tx_hash', 'evt_index', 'bridge_transfer_id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set bridges_platforms = [
'bridges_' + blockchain + '_cctp_v1_withdrawals'
    , 'bridges_' + blockchain + '_across_v2_withdrawals'
    , 'bridges_' + blockchain + '_across_v3_withdrawals'
    , 'bridges_' + blockchain + '_hyperliquid_v1_withdrawals'
    , 'bridges_' + blockchain + '_hyperliquid_v2_withdrawals'
    , 'bridges_' + blockchain + '_synapse_rfq_withdrawals'
    , 'bridges_' + blockchain + '_celer_v1_withdrawals'
    , 'bridges_' + blockchain + '_tether_v1_withdrawals'
] %}

SELECT *
FROM (
        {% for bridge_platform in bridges_platforms %}
        SELECT w.deposit_chain_id
            , w.deposit_chain
            , w.withdrawal_chain
            , w.bridge_name
            , w.bridge_version
            , w.block_date
            , w.block_time
            , w.block_number
            , w.withdrawal_amount_raw
            , w.sender
            , w.recipient
            , w.withdrawal_token_address
            , w.withdrawal_token_standard
            , w.tx_from
            , w.tx_hash
            , w.evt_index
            , w.contract_address
            , w.bridge_transfer_id
        FROM {{ ref(bridge_platform) }} w
        {% if is_incremental() %}
        LEFT JOIN {{this}} t ON t.withdrawal_chain = '{{chain}}'
            AND w.bridge_name = t.bridge_name
            AND w.bridge_version = t.bridge_version
            AND w.deposit_chain_id = t.deposit_chain_id
            AND w.tx_hash = t.tx_hash
            AND w.evt_index = t.evt_index
            AND w.bridge_transfer_id = t.bridge_transfer_id
        WHERE  {{ incremental_predicate('w.block_time') }}
        AND t.block_time IS NULL
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
