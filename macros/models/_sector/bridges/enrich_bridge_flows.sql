{% macro enrich_bridge_flows(base_flows) %}
-- Macro to apply the Bridge flows enrichment(s) to base models

SELECT bf.*
FROM {{base_flows}} bf
{% if is_incremental() %}
LEFT JOIN {{this}} t ON t.blockchain=bf.blockchain
    AND t.tx_hash=bf.tx_hash
    AND t.evt_index=bf.evt_index
    AND t.block_number IS NULL
WHERE {{ incremental_predicate('block_time') }}
{% endif %}

{% endmacro %}
