{% macro enrich_bridge_flows(base_flows) %}

-- first single out all destination flows (received bridged funds) and try to match them with corresponding source flows
WITH received_flows AS (
    SELECT source.blockchain AS source_blockchain
    , destination.blockchain AS destination_blockchain
    , destination.project
    , destination.project_version
    , source.block_time AS source_block_time
    , source.block_month AS source_block_month
    , source.block_number AS source_block_number
    , source.amount_raw AS source_amount_raw
    , destination.block_time AS destination_block_time
    , destination.block_month AS destination_block_month
    , destination.block_number AS destination_block_number
    , destination.amount_raw AS destination_amount_raw
    , destination.sender
    , destination.recipient
    , destination.local_token AS source_token_address
    , destination.remote_token AS destination_token_address
    , destination.extra_data
    , source.tx_hash AS source_tx_hash
    , source.evt_index AS source_evt_index
    , destination.tx_hash AS destination_tx_hash
    , destination.evt_index AS destination_evt_index
    , source.contract_address AS source_contract_address
    , destination.contract_address AS destination_contract_address
    FROM {{base_flows}} destination
    {% if is_incremental() %}
    LEFT JOIN {{this}} t ON t.blockchain=destination.blockchain
        AND t.tx_hash=destination.tx_hash
        AND t.evt_index=destination.evt_index
        AND t.block_number IS NULL
    {% endif %}
    LEFT JOIN {{base_flows}} source ON source.flows_type='sent'
        AND source.blockchain!=destination.blockchain
        AND source.project=destination.project
        AND source.sender=destination.sender
        AND source.recipient=destination.recipient
        AND source.local_token=destination.local_token
        AND source.remote_token=destination.remote_token
        AND source.project_version=destination.project_version
        AND source.block_time<destination.block_time
        AND source.extra_data=destination.extra_data
        {% if is_incremental() %}
        AND {{ incremental_predicate('source.block_time') }}
        {% endif %}
    WHERE destination.flows_type='received'
    {% if is_incremental() %}
    AND {{ incremental_predicate('destination.block_time') }}
    {% endif %}
    )

-- track all source flows without a matching destination flow
, sent_flows AS (
    SELECT source.blockchain AS source_blockchain
    , NULL AS destination_blockchain
    , source.project
    , source.project_version
    , source.block_time AS source_block_time
    , source.block_month AS source_block_month
    , source.block_number AS source_block_number
    , source.amount_raw AS source_amount_raw
    , NULL AS destination_block_time
    , NULL AS destination_block_month
    , CAST(NULL AS UINT256) AS destination_block_number
    , CAST(NULL AS UINT256) AS destination_amount_raw
    , source.sender
    , source.recipient
    , source.local_token AS source_token_address
    , source.remote_token AS destination_token_address
    , source.extra_data
    , source.tx_hash AS source_tx_hash
    , source.evt_index AS source_evt_index
    , CAST(NULL AS varbinary) AS destination_tx_hash
    , CAST(NULL AS UINT256) AS destination_evt_index
    , source.contract_address AS source_contract_address
    , CAST(NULL AS varbinary) AS destination_contract_address
    FROM {{base_flows}} source
    {% if is_incremental() %}
    LEFT JOIN {{this}} t ON t.blockchain=destination.blockchain
        AND t.tx_hash=destination.tx_hash
        AND t.evt_index=destination.evt_index
        AND t.block_number IS NULL
    {% endif %}
    LEFT JOIN received_flows rf ON rf.source_blockchain=source.blockchain
        AND rf.source_tx_hash=source.tx_hash
        AND rf.source_evt_index=source.evt_index
        AND rf.source_blockchain IS NULL
    WHERE source.flows_type='sent'
    -- buffer to wait for potential destination tx to show up
    AND source.block_time < NOW() - interval '6' hour
    {% if is_incremental() %}
    AND {{ incremental_predicate('source.block_time') }}
    {% endif %}
    )

SELECT source_blockchain
, destination_blockchain
, project
, project_version
, source_block_time
, source_block_month
, source_block_number
, source_amount_raw
, destination_block_time
, destination_block_month
, destination_block_number
, destination_amount_raw
, sender
, recipient
, source_token_address
, destination_token_address
, extra_data
, source_tx_hash
, source_evt_index
, destination_tx_hash
, destination_evt_index
, source_contract_address
, destination_contract_address
, destination_blockchain || '-' || CAST (destination_tx_hash AS varchar) || '-' CAST (destination_evt_index AS varchar) AS unique_identifier
, destination_block_time AS last_updated
FROM received_flows

UNION ALL


SELECT source_blockchain
, destination_blockchain
, project
, project_version
, source_block_time
, source_block_month
, source_block_number
, source_amount_raw
, destination_block_time
, destination_block_month
, destination_block_number
, destination_amount_raw
, sender
, recipient
, source_token_address
, destination_token_address
, extra_data
, source_tx_hash
, source_evt_index
, destination_tx_hash
, destination_evt_index
, source_contract_address
, destination_contract_address
, source_blockchain || '-' || CAST (source_tx_hash AS varchar) || '-' CAST (source_evt_index AS varchar) AS unique_identifier
, source_block_time AS last_updated
FROM sent_flows
{% endmacro %}