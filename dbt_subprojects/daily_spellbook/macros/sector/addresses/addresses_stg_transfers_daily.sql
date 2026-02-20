{% macro addresses_stg_transfers_daily_received(token_transfers) %}
{# Single read from transfers: received side only. Grain: address, address_prefix, block_date. Same incremental/filter as daily. #}
select
	tt."to" as address
	, varbinary_to_integer(varbinary_substring(tt."to", 1, 1)) as address_prefix
	, tt.block_date
	, cast(date_trunc('month', tt.block_date) as date) as block_month
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
where tt.block_date < cast(now() as date)
{% if is_incremental() -%}
and {{ incremental_predicate('tt.block_date') }}
{% endif -%}
group by
	tt."to"
	, 2
	, tt.block_date
{% endmacro %}


{% macro addresses_stg_transfers_daily_sent(token_transfers) %}
{# Single read from transfers: sent side only. Grain: address, address_prefix, block_date. Same incremental/filter as daily. #}
select
	tt."from" as address
	, varbinary_to_integer(varbinary_substring(tt."from", 1, 1)) as address_prefix
	, tt.block_date
	, cast(date_trunc('month', tt.block_date) as date) as block_month
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
where tt.block_date < cast(now() as date)
{% if is_incremental() -%}
and {{ incremental_predicate('tt.block_date') }}
{% endif -%}
group by
	tt."from"
	, 2
	, tt.block_date
{% endmacro %}


{% macro addresses_stg_transfers_daily_union(received_model, sent_model) %}
{# Union received + sent daily staging and aggregate to same grain. No read from transfers. Incremental: filter by block_date when merging. #}
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
		r.address
		, r.address_prefix
		, r.block_date
		, r.block_month
		, r.tokens_received_count
		, r.tokens_received_tx_count
		, r.tokens_sent_count
		, r.tokens_sent_tx_count
		, r.first_transfer_block_time
		, r.last_transfer_block_time
		, r.first_received_block_number
		, r.last_received_block_number
		, r.first_sent_block_number
		, r.last_sent_block_number
		, r.received_volume_usd
		, r.sent_volume_usd
	from
		{{ received_model }} as r
	{% if is_incremental() -%}
	where {{ incremental_predicate('r.block_date') }}
	{% endif -%}
	union all
	select
		s.address
		, s.address_prefix
		, s.block_date
		, s.block_month
		, s.tokens_received_count
		, s.tokens_received_tx_count
		, s.tokens_sent_count
		, s.tokens_sent_tx_count
		, s.first_transfer_block_time
		, s.last_transfer_block_time
		, s.first_received_block_number
		, s.last_received_block_number
		, s.first_sent_block_number
		, s.last_sent_block_number
		, s.received_volume_usd
		, s.sent_volume_usd
	from
		{{ sent_model }} as s
	{% if is_incremental() -%}
	where {{ incremental_predicate('s.block_date') }}
	{% endif -%}
)
group by
	1
	, 2
	, 3
{% endmacro %}
