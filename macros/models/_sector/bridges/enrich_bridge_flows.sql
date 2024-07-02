{% macro enrich_bridge_flows(base_flows) %}
-- Macro to apply the Bridge flows enrichment(s) to base models

SELECT bf.blockchain
, bf.project
, bf.project_version
, bf.flows_type
, bf.block_time
, bf.block_month
, bf.block_number
, bf.amount_raw
, bf.sender
, bf.recipient
, bf.local_token
, bf.remote_token
, bf.extra_data
, bf.tx_hash
, bf.evt_index
, bf.contract_address
FROM {{base_flows}} bf
{% if is_incremental() %}
LEFT JOIN {{this}} t ON t.blockchain=bf.blockchain
    AND t.tx_hash=bf.tx_hash
    AND t.evt_index=bf.evt_index
    AND t.block_number IS NULL
WHERE {{ incremental_predicate('bf.block_time') }}
{% endif %}

{% endmacro %}
