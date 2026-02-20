{% macro addresses_stg_executed_txs_daily(transactions) %}
{# Incremental by block_date: rolling window, full days only (exclude current day). Grain: address, address_prefix, block_date. Partition: block_month. #}
select
	txs."from" as address
	, varbinary_to_integer(varbinary_substring(txs."from", 1, 1)) as address_prefix
	, txs.block_date as block_date
	, cast(date_trunc('month', txs.block_date) as date) as block_month
	, count(*) as executed_tx_count
	, coalesce(max(txs.nonce), 0) as max_nonce
	, min(txs.block_time) as first_tx_block_time
	, max(txs.block_time) as last_tx_block_time
	, min(txs.block_number) as first_tx_block_number
	, max(txs.block_number) as last_tx_block_number
from
	{{ transactions }} as txs
where txs.block_date < cast(now() as date)
{% if is_incremental() or true -%}
and {{ incremental_predicate('txs.block_date') }}
{% endif -%}
group by
	1
	, 2
	, 3
{% endmacro %}
