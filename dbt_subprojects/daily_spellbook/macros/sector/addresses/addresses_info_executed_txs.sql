{% macro addresses_info_executed_txs(transactions) %}
select
	txs."from" as address
	, varbinary_to_integer(varbinary_substring(txs."from", 1, 1)) as address_prefix
	, count(*) as executed_tx_count
	, coalesce(max(txs.nonce), 0) as max_nonce
	, min(txs.block_time) as first_tx_block_time
	, max(txs.block_time) as last_tx_block_time
	, min(txs.block_number) as first_tx_block_number
	, max(txs.block_number) as last_tx_block_number
from
	{{ transactions }} as txs
{% if is_incremental() -%}
where {{ incremental_predicate('txs.block_time') }}
{% endif -%}
group by
	1
	, 2
{% endmacro %}
