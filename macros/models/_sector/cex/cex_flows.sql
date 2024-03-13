{% macro cex_flows(blockchain, transfers, addresses) %}

SELECT '{{blockchain}}' AS blockchain
, CAST(date_trunc('month', block_time) AS date) AS block_month
, block_time
, block_number
, a.cex_name
, a.distinct_name
, t.contract_address AS token_address
, t.symbol AS token_symbol
, t.token_standard
, CASE WHEN a.address=t."from" THEN 'Outflow' ELSE 'Inflow' END AS flow_type
, CASE WHEN a.address=t."from" THEN -t.amount ELSE t.amount END AS amount
, t.amount_raw
, CASE WHEN a.address=t."from" THEN -t.amount_usd ELSE t.amount_usd END AS amount_usd
, t."from"
, t.to
, t.tx_from
, t.tx_to
, t.tx_index
, t.tx_hash
, t.evt_index
, t.unique_key
FROM {{transfers}} t
INNER JOIN {{addresses}} a ON a.address = t."from" OR a.address = t.to
{% if is_incremental() %}
WHERE {{incremental_predicate('block_time')}}
{% endif %}

{% endmacro %}