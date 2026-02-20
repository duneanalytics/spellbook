{% macro addresses_stg_transfers_daily(token_transfers) %}
{# Incremental by block_date: rolling window, full days only (exclude current day). Grain: address, address_prefix, block_date. Partition: block_month. #}
select
	address
	, address_prefix
	, block_date
	, cast(date_trunc('month', block_date) as date) as block_month
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
		, cast(tt.block_time as date) as block_date
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
	where cast(tt.block_time as date) < cast(now() as date)
	{% if is_incremental() -%}
	and {{ incremental_predicate('cast(tt.block_time as date)') }}
	{% endif -%}
	group by
		tt."from"
		, 2
		, cast(tt.block_time as date)

	union all

	select
		tt."to" as address
		, varbinary_to_integer(varbinary_substring(tt."to", 1, 1)) as address_prefix
		, cast(tt.block_time as date) as block_date
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
	where cast(tt.block_time as date) < cast(now() as date)
	{% if is_incremental() -%}
	and {{ incremental_predicate('cast(tt.block_time as date)') }}
	{% endif -%}
	group by
		tt."to"
		, 2
		, cast(tt.block_time as date)
	)
group by
	1
	, 2
	, 3
{% endmacro %}
