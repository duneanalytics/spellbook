{% macro enrich_bridge_flows(base_flows) %}
-- Macro to apply the Bridge flows enrichment(s) to base models

SELECT *
FROM {{base_flows}}

{% endmacro %}
