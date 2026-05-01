{% macro addresses_info_from_staging(blockchain, staging_model) %}
{# Staging is full-history view (join of stg_transfers, stg_executed_txs, is_contract, ffb). Pass-through to build chain info table. #}
select
	nd.blockchain
	, nd.address
	, nd.address_prefix
	, nd.executed_tx_count
	, nd.max_nonce
	, nd.is_smart_contract
	, nd.namespace
	, nd.name
	, nd.first_funded_by
	, nd.first_funded_by_block_time
	, nd.tokens_received_count
	, nd.tokens_received_tx_count
	, nd.tokens_sent_count
	, nd.tokens_sent_tx_count
	, nd.first_transfer_block_time
	, nd.last_transfer_block_time
	, nd.first_received_block_number
	, nd.last_received_block_number
	, nd.first_sent_block_number
	, nd.last_sent_block_number
	, nd.received_volume_usd
	, nd.sent_volume_usd
	, nd.first_tx_block_time
	, nd.last_tx_block_time
	, nd.first_tx_block_number
	, nd.last_tx_block_number
	, nd.last_seen
	, nd.last_seen_block
from
	{{ staging_model }} as nd
{% endmacro %}
