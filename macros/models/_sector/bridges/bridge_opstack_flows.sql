{% macro bridge_opstack_flows(blockchain, project, project_version, events, token_standard, flows_type)
%}

SELECT '{{blockchain}}' AS blockchain
, '{{blockchain}}' AS project
, project_version
, '{{flows_type}}' AS flows_type
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS amount_raw
, "from"
, to
, COALESCE(try(localToken), 0x0000000000000000000000000000000000000000) AS local_token
, COALESCE(try(remoteToken), 0x0000000000000000000000000000000000000000) AS remote_token
, extraData AS extra_data
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{events}}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% endif %}


{% endmacro %}
