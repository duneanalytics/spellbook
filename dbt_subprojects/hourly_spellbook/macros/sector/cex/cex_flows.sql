{% macro cex_flows(blockchain, transfers, addresses) %}

SELECT DISTINCT '{{blockchain}}' AS blockchain
, CAST(date_trunc('month', block_time) AS date) AS block_month
, block_time
, block_number
, a.cex_name
, a.distinct_name
, t.contract_address AS token_address
, t.symbol AS token_symbol
, t.token_standard
, CASE WHEN a.cex_name=b.cex_name AND a.cex_name IS NOT NULL AND a.address=t."from" THEN 'Internal'
    WHEN a.cex_name<>b.cex_name AND a.cex_name IS NOT NULL AND b.cex_name IS NOT NULL AND a.address=t."from" THEN 'Cross-CEX'
    WHEN b.cex_name IS NOT NULL THEN 'Inflow'
    WHEN a.cex_name IS NOT NULL AND a.address=t."from" THEN 'Outflow'
    WHEN a.cex_name IS NOT NULL THEN 'Executed'
    END AS flow_type
, CASE WHEN a.address=t."from" AND b.address!=t.to THEN -t.amount ELSE t.amount END AS amount
, t.amount_raw
, CASE WHEN a.address=t."from" AND b.address!=t.to THEN -t.amount_usd ELSE t.amount_usd END AS amount_usd
, t."from"
, t.to
, t.tx_from
, t.tx_to
, t.tx_index
, t.tx_hash
, t.evt_index
, t.unique_key
FROM {{transfers}} t
LEFT JOIN cex_ethereum.addresses a ON a.address = t."from" OR a.address=t.tx_from
LEFT JOIN cex_ethereum.addresses b ON b.address = t.to
WHERE a.cex_name IS NOT NULL OR b.cex_name IS NOT NULL
{% if is_incremental() %}
AND {{incremental_predicate('block_time')}}
{% endif %}

{% endmacro %}