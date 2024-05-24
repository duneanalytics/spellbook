{% macro gas_prices(blockchain, transactions) %}

SELECT blockchain AS blockchain
, date_trunc('minute', block_time) AS time
, approx_percentile(gas_price, 0.1) AS median_gas
, approx_percentile(gas_price, 0.5) AS tenth_percentile_gas
, approx_percentile(gas_price, 0.9) AS ninetieth_percentile_gas
, AVG(gas_price) AS avg_gas
, MIN(gas_price) AS min_gas
, MAX(gas_price) AS max_gas
FROM {{transactions}}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
GROUP BY 1

{% endmacro %}
