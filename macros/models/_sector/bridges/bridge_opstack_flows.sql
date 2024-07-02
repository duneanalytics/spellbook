{% macro bridge_opstack_flows(blockchain, project, project_version, events, token_standard, flows_type)
%}

{% if token_standard == 'native' %}
SELECT '{{blockchain}}' AS blockchain
, '{{project}}' AS project
, '{{project_version}}' AS project_version
, '{{flows_type}}' AS flows_type
, e.evt_block_time AS block_time
, date_trunc('month', e.evt_block_time) AS block_month
, e.evt_block_number AS block_number
, e.amount AS amount_raw
, e."from" AS sender
, e.to AS recipient
, {{ var("ETH_ERC20_ADDRESS") }} AS local_token
, {{ var("ETH_ERC20_ADDRESS") }} AS remote_token
, e.extraData AS extra_data
, e.evt_tx_hash AS tx_hash
, e.evt_index
, e.contract_address
FROM {{events}} e
{% if is_incremental() %}
LEFT JOIN {{this}} t ON t.tx_hash=e.evt_tx_hash AND t.evt_index=e.evt_index
    AND t.blockchain IS NULL
WHERE {{incremental_predicate('evt_block_time')}}
{% endif %}

{% else %}
SELECT '{{blockchain}}' AS blockchain
, '{{project}}' AS project
, '{{project_version}}' AS project_version
, '{{flows_type}}' AS flows_type
, e.evt_block_time AS block_time
, date_trunc('month', e.evt_block_time) AS block_month
, e.evt_block_number AS block_number
, e.amount AS amount_raw
, e."from" AS sender
, e.to AS recipient
, e.localToken AS local_token
, e.remotetoken AS remote_token
, e.extraData AS extra_data
, e.evt_tx_hash AS tx_hash
, e.evt_index
, e.contract_address
FROM {{events}} e
{% if is_incremental() %}
LEFT JOIN {{this}} t ON t.tx_hash=e.evt_tx_hash AND t.evt_index=e.evt_index
    AND t.blockchain IS NULL
WHERE {{incremental_predicate('evt_block_time')}}
{% endif %}
{% endif %}

{% endmacro %}
