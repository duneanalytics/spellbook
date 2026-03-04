{% macro addresses_stg_executed_txs_agg(daily_model) %}
{# Full table: aggregate daily staging to address + address_prefix only. For max_nonce we take max over days. #}
select
	d.address
	, d.address_prefix
	, sum(d.executed_tx_count) as executed_tx_count
	, max(d.max_nonce) as max_nonce
	, min(d.first_tx_block_time) as first_tx_block_time
	, max(d.last_tx_block_time) as last_tx_block_time
	, min(d.first_tx_block_number) as first_tx_block_number
	, max(d.last_tx_block_number) as last_tx_block_number
from
	{{ daily_model }} as d
group by
	d.address
	, d.address_prefix
{% endmacro %}
