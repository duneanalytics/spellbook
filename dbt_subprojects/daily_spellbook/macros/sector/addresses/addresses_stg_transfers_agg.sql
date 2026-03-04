{% macro addresses_stg_transfers_agg(daily_model) %}
{# Full table: aggregate daily staging to address + address_prefix only. Read from addresses_<chain>_stg_transfers_daily. #}
select
	d.address
	, d.address_prefix
	, sum(d.tokens_received_count) as tokens_received_count
	, sum(d.tokens_received_tx_count) as tokens_received_tx_count
	, sum(d.tokens_sent_count) as tokens_sent_count
	, sum(d.tokens_sent_tx_count) as tokens_sent_tx_count
	, min(d.first_transfer_block_time) as first_transfer_block_time
	, max(d.last_transfer_block_time) as last_transfer_block_time
	, min(d.first_received_block_number) as first_received_block_number
	, max(d.last_received_block_number) as last_received_block_number
	, min(d.first_sent_block_number) as first_sent_block_number
	, max(d.last_sent_block_number) as last_sent_block_number
	, sum(d.received_volume_usd) as received_volume_usd
	, sum(d.sent_volume_usd) as sent_volume_usd
from
	{{ daily_model }} as d
group by
	d.address
	, d.address_prefix
{% endmacro %}
