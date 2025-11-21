{% macro cex_flows(blockchain, transfers, addresses) %}

SELECT '{{blockchain}}' AS blockchain
, CAST(date_trunc('month', block_time) AS date) AS block_month
, block_time
, block_number
, COALESCE(c.cex_name, a.cex_name, d.cex_name, b.cex_name) AS cex_name
, a.distinct_name
, t.contract_address AS token_address
, t.symbol AS token_symbol
, t.token_standard
, CASE WHEN a.cex_name=c.cex_name AND a.cex_name IS NOT NULL THEN 'Internal'
    WHEN a.cex_name<>c.cex_name AND a.cex_name IS NOT NULL AND c.cex_name IS NOT NULL THEN 'Cross-CEX'
    WHEN a.cex_name IS NOT NULL THEN 'Outflow'
    WHEN c.cex_name IS NOT NULL THEN 'Inflow'
    WHEN d.cex_name IS NOT NULL THEN 'Executed Contract'
    WHEN b.cex_name IS NOT NULL THEN 'Executed'
    END AS flow_type
, t.amount
, t.amount_raw
, t.amount_usd
, t."from"
, t.to
, t.tx_from
, t.tx_to
, t.tx_index
, t.tx_hash
, t.evt_index
, t.unique_key
FROM {{transfers}} t
LEFT JOIN {{addresses}} a ON a.address = t."from" AND t.block_time>=a.first_used
LEFT JOIN {{addresses}} b ON b.address = t.tx_from AND t.block_time>=b.first_used
LEFT JOIN {{addresses}} c ON c.address = t.to AND t.block_time>=c.first_used
LEFT JOIN {{addresses}} d ON d.address = t.tx_to AND t.block_time>=d.first_used
WHERE (a.cex_name IS NOT NULL OR c.cex_name IS NOT NULL OR b.cex_name IS NOT NULL OR d.cex_name IS NOT NULL)
{% if is_incremental() %}
AND {{incremental_predicate('block_time')}}
{% endif %}

{% endmacro %}