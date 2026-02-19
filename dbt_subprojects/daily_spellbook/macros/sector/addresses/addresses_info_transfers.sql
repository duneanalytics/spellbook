{% macro addresses_info_transfers(token_transfers) %}
select
	address
	, address_prefix
	, sum(coalesce(tokens_received_count, 0)) as tokens_received_count
	, sum(coalesce(tokens_received_tx_count, 0)) as tokens_received_tx_count
	, sum(coalesce(tokens_sent_count, 0)) as tokens_sent_count
	, sum(coalesce(tokens_sent_tx_count, 0)) as tokens_sent_tx_count
	, min(first_transfer_block_time) as first_transfer_block_time
	, max(last_transfer_block_time) as last_transfer_block_time
	, min(first_received_block_number) as first_received_block_number
	, max(last_received_block_number) as last_received_block_number
	, min(first_sent_block_number) as first_sent_block_number
	, max(last_sent_block_number) as last_sent_block_number
	, sum(received_volume_usd) as received_volume_usd
	, sum(sent_volume_usd) as sent_volume_usd
from (
	select
		tt."from" as address
		, varbinary_to_integer(varbinary_substring(tt."from", 1, 1)) as address_prefix
		, 0 as tokens_received_count
		, 0 as tokens_received_tx_count
		, count(*) as tokens_sent_count
		, count(distinct tt.tx_hash) as tokens_sent_tx_count
		, min(tt.block_time) as first_transfer_block_time
		, max(tt.block_time) as last_transfer_block_time
		, min(tt.block_number) as first_received_block_number
		, max(tt.block_number) as last_received_block_number
		, cast(null as bigint) as first_sent_block_number
		, cast(null as bigint) as last_sent_block_number
		, 0 as received_volume_usd
		, sum(tt.amount_usd) as sent_volume_usd
	from
		{{ token_transfers }} as tt
	{% if is_incremental() or true -%}
	where {{ incremental_predicate('tt.block_time') }}
	{% endif -%}
	group by
		tt."from"
		, 2

	union all

	select
		tt."to" as address
		, varbinary_to_integer(varbinary_substring(tt."to", 1, 1)) as address_prefix
		, count(*) as tokens_received_count
		, count(distinct tt.tx_hash) as tokens_received_tx_count
		, 0 as tokens_sent_count
		, 0 as tokens_sent_tx_count
		, min(tt.block_time) as first_transfer_block_time
		, max(tt.block_time) as last_transfer_block_time
		, cast(null as bigint) as first_received_block_number
		, cast(null as bigint) as last_received_block_number
		, min(tt.block_number) as first_sent_block_number
		, max(tt.block_number) as last_sent_block_number
		, sum(tt.amount_usd) as received_volume_usd
		, 0 as sent_volume_usd
	from
		{{ token_transfers }} as tt
	{% if is_incremental() or true -%}
	where {{ incremental_predicate('tt.block_time') }}
	{% endif -%}
	group by
		tt."to"
		, 2
	)
group by
	1
	, 2
{% endmacro %}
