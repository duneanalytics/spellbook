{% macro addresses_info(blockchain, transactions, token_transfers, creation_traces, first_funded_by, contracts) %}
-- Shared aggregation logic. When is_incremental(), applies incremental_predicate on source reads (for staging model).
WITH executed_txs AS (
	SELECT txs."from" AS address
	, varbinary_to_integer(varbinary_substring(txs."from", 1, 1)) AS address_prefix
	, COUNT(*) AS executed_tx_count
	, COALESCE(MAX(txs.nonce), 0) AS max_nonce
	, MIN(txs.block_time) AS first_tx_block_time
	, MAX(txs.block_time) AS last_tx_block_time
	, MIN(txs.block_number) AS first_tx_block_number
	, MAX(txs.block_number) AS last_tx_block_number
	FROM {{ transactions }} AS txs
	{% if is_incremental() -%}
	WHERE {{ incremental_predicate('txs.block_time') }}
	{% endif -%}
	GROUP BY 1, 2
	)

, transfers AS (
	SELECT address
	, address_prefix
	, SUM(COALESCE(tokens_received_count, 0)) AS tokens_received_count
	, SUM(COALESCE(tokens_received_tx_count, 0)) AS tokens_received_tx_count
	, SUM(COALESCE(tokens_sent_count, 0)) AS tokens_sent_count
	, SUM(COALESCE(tokens_sent_tx_count, 0)) AS tokens_sent_tx_count
	, MIN(first_transfer_block_time) AS first_transfer_block_time
	, MAX(last_transfer_block_time) AS last_transfer_block_time
	, MIN(first_received_block_number) AS first_received_block_number
	, MAX(last_received_block_number) AS last_received_block_number
	, MIN(first_sent_block_number) AS first_sent_block_number
	, MAX(last_sent_block_number) AS last_sent_block_number
	, SUM(received_volume_usd) AS received_volume_usd
	, SUM(sent_volume_usd) AS sent_volume_usd
	FROM (
		SELECT tt."from" AS address
		, varbinary_to_integer(varbinary_substring(tt."from", 1, 1)) AS address_prefix
		, 0 AS tokens_received_count
		, 0 AS tokens_received_tx_count
		, COUNT(*) AS tokens_sent_count
		, COUNT(DISTINCT tt.tx_hash) AS tokens_sent_tx_count
		, MIN(tt.block_time) AS first_transfer_block_time
		, MAX(tt.block_time) AS last_transfer_block_time
		, MIN(tt.block_number) AS first_received_block_number
		, MAX(tt.block_number) AS last_received_block_number
		, CAST(NULL AS bigint) AS first_sent_block_number
		, CAST(NULL AS bigint) AS last_sent_block_number
		, 0 AS received_volume_usd
		, SUM(tt.amount_usd) AS sent_volume_usd
		FROM {{ token_transfers }} AS tt
		{% if is_incremental() -%}
		WHERE {{ incremental_predicate('tt.block_time') }}
		{% endif -%}
		GROUP BY tt."from", 2

		UNION ALL

		SELECT tt."to" AS address
		, varbinary_to_integer(varbinary_substring(tt."to", 1, 1)) AS address_prefix
		, COUNT(*) AS tokens_received_count
		, COUNT(DISTINCT tt.tx_hash) AS tokens_received_tx_count
		, 0 AS tokens_sent_count
		, 0 AS tokens_sent_tx_count
		, MIN(tt.block_time) AS first_transfer_block_time
		, MAX(tt.block_time) AS last_transfer_block_time
		, CAST(NULL AS bigint) AS first_received_block_number
		, CAST(NULL AS bigint) AS last_received_block_number
		, MIN(tt.block_number) AS first_sent_block_number
		, MAX(tt.block_number) AS last_sent_block_number
		, SUM(tt.amount_usd) AS received_volume_usd
		, 0 AS sent_volume_usd
		FROM {{ token_transfers }} AS tt
		{% if is_incremental() -%}
		WHERE {{ incremental_predicate('tt.block_time') }}
		{% endif -%}
		GROUP BY tt."to", 2
		)
	GROUP BY 1, 2
	)

, is_contract AS (
	SELECT ct.address
	, varbinary_to_integer(varbinary_substring(ct.address, 1, 1)) AS address_prefix
	, true AS is_smart_contract
	, MAX_BY(c.namespace, c.created_at) AS namespace
	, MAX_BY(c.name, c.created_at) AS name
	FROM {{ creation_traces }} AS ct
	LEFT JOIN {{ contracts }} AS c ON ct.address = c.address
	{% if is_incremental() -%}
	WHERE {{ incremental_predicate('ct.block_time') }}
	{% endif -%}
	GROUP BY 1, 2
	)

SELECT '{{ blockchain }}' AS blockchain
, address
, address_prefix
, COALESCE(executed_tx_count, 0) AS executed_tx_count
, max_nonce AS max_nonce
, COALESCE(is_smart_contract, false) AS is_smart_contract
, namespace
, name
, first_funded_by
, ffb.block_time AS first_funded_by_block_time
, tokens_received_count
, tokens_received_tx_count
, tokens_sent_count
, tokens_sent_tx_count
, first_transfer_block_time
, last_transfer_block_time
, first_received_block_number
, last_received_block_number
, first_sent_block_number
, last_sent_block_number
, received_volume_usd
, sent_volume_usd
, first_tx_block_time
, last_tx_block_time
, first_tx_block_number
, last_tx_block_number
, ARRAY_MAX(FILTER(ARRAY[last_tx_block_time, last_transfer_block_time], x -> x IS NOT NULL)) AS last_seen
, ARRAY_MAX(FILTER(ARRAY[last_tx_block_number, last_received_block_number, last_sent_block_number], x -> x IS NOT NULL)) AS last_seen_block
FROM transfers
FULL OUTER JOIN executed_txs USING (address, address_prefix)
LEFT JOIN (
	SELECT *
	, varbinary_to_integer(varbinary_substring(address, 1, 1)) AS address_prefix
	FROM {{ source('addresses_events_' ~ blockchain, 'first_funded_by') }}
) AS ffb USING (address, address_prefix)
LEFT JOIN is_contract ic USING (address, address_prefix)
WHERE address IS NOT NULL
{% endmacro %}
