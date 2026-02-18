{% macro addresses_info_incremental(blockchain, staging_model) %}
-- Single join to target. Staging contains only addresses with activity in incremental window.
SELECT '{{ blockchain }}' AS blockchain
, nd.address
, nd.address_prefix
, nd.executed_tx_count + COALESCE(t.executed_tx_count, 0) AS executed_tx_count
, COALESCE(nd.max_nonce, t.max_nonce) AS max_nonce
, COALESCE(nd.is_smart_contract, t.is_smart_contract) AS is_smart_contract
, COALESCE(nd.namespace, t.namespace) AS namespace
, COALESCE(nd.name, t.name) AS name
, COALESCE(t.first_funded_by, nd.first_funded_by) AS first_funded_by
, COALESCE(t.first_funded_by_block_time, nd.first_funded_by_block_time) AS first_funded_by_block_time
, COALESCE(nd.tokens_received_count, 0) + COALESCE(t.tokens_received_count, 0) AS tokens_received_count
, COALESCE(nd.tokens_received_tx_count, 0) + COALESCE(t.tokens_received_tx_count, 0) AS tokens_received_tx_count
, COALESCE(nd.tokens_sent_count, 0) + COALESCE(t.tokens_sent_count, 0) AS tokens_sent_count
, COALESCE(nd.tokens_sent_tx_count, 0) + COALESCE(t.tokens_sent_tx_count, 0) AS tokens_sent_tx_count
, COALESCE(t.first_transfer_block_time, nd.first_transfer_block_time) AS first_transfer_block_time
, COALESCE(nd.last_transfer_block_time, t.last_transfer_block_time) AS last_transfer_block_time
, COALESCE(t.first_received_block_number, nd.first_received_block_number) AS first_received_block_number
, COALESCE(nd.last_received_block_number, t.last_received_block_number) AS last_received_block_number
, COALESCE(t.first_sent_block_number, nd.first_sent_block_number) AS first_sent_block_number
, COALESCE(nd.last_sent_block_number, t.last_sent_block_number) AS last_sent_block_number
, COALESCE(nd.received_volume_usd, 0) + COALESCE(t.received_volume_usd, 0) AS received_volume_usd
, COALESCE(nd.sent_volume_usd, 0) + COALESCE(t.sent_volume_usd, 0) AS sent_volume_usd
, COALESCE(t.first_tx_block_time, nd.first_tx_block_time) AS first_tx_block_time
, COALESCE(nd.last_tx_block_time, t.last_tx_block_time) AS last_tx_block_time
, COALESCE(t.first_tx_block_number, nd.first_tx_block_number) AS first_tx_block_number
, COALESCE(nd.last_tx_block_number, t.last_tx_block_number) AS last_tx_block_number
, ARRAY_MAX(FILTER(ARRAY[nd.last_tx_block_time, nd.last_transfer_block_time, t.last_seen], x -> x IS NOT NULL)) AS last_seen
, ARRAY_MAX(FILTER(ARRAY[nd.last_tx_block_number, nd.last_received_block_number, nd.last_sent_block_number, t.last_seen_block], x -> x IS NOT NULL)) AS last_seen_block
FROM (
	SELECT *
	FROM {{ staging_model }} AS nd
	WHERE {{ incremental_predicate('nd.last_seen') }}
	) AS nd
LEFT JOIN {{ this }} AS t
	ON t.address_prefix = nd.address_prefix
	AND t.address = nd.address
{% endmacro %}
