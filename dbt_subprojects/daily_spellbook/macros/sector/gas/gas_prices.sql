{% macro gas_prices(blockchain, transactions) %}

SELECT '{{blockchain}}' AS blockchain
, date_trunc('minute', block_time) AS minute
, approx_percentile(gas_price/1e9, 0.1) AS tenth_percentile_gas
, approx_percentile(gas_price/1e9, 0.5) AS median_gas
, approx_percentile(gas_price/1e9, 0.9) AS ninetieth_percentile_gas
, AVG(gas_price/1e9) AS avg_gas
, MIN(gas_price/1e9) AS min_gas
, MAX(gas_price/1e9) AS max_gas
FROM {{transactions}}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
GROUP BY 2

{% endmacro %}
