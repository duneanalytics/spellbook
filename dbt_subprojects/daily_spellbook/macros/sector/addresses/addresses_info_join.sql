{% macro addresses_info_join(blockchain, executed_txs_model, transfers_model, is_contract_model) %}
with transfers as (
	select
		*
	from
		{{ transfers_model }}
	{% if is_incremental() -%}
	where {{ incremental_predicate('last_transfer_block_time') }}
	{% endif -%}
)
, executed_txs as (
	select
		*
	from
		{{ executed_txs_model }}
	{% if is_incremental() -%}
	where {{ incremental_predicate('last_tx_block_time') }}
	{% endif -%}
)
, ffb as (
	select
		*
		, varbinary_to_integer(varbinary_substring(address, 1, 1)) as address_prefix
	from
		{{ source('addresses_events_' ~ blockchain, 'first_funded_by') }}
)
, is_contract as (
	select
		*
	from
		{{ is_contract_model }}
	{% if is_incremental() -%}
	where {{ incremental_predicate('block_time') }}
	{% endif -%}
)

select
	'{{ blockchain }}' as blockchain
	, coalesce(tr.address, et.address) as address
	, coalesce(tr.address_prefix, et.address_prefix) as address_prefix
	, coalesce(et.executed_tx_count, 0) as executed_tx_count
	, et.max_nonce as max_nonce
	, {% if is_incremental() %}ic.is_smart_contract{% else %}coalesce(ic.is_smart_contract, false){% endif %} as is_smart_contract
	, ic.namespace as namespace
	, ic.name as name
	, ffb.first_funded_by as first_funded_by
	, ffb.block_time as first_funded_by_block_time
	, coalesce(tr.tokens_received_count, 0) as tokens_received_count
	, coalesce(tr.tokens_received_tx_count, 0) as tokens_received_tx_count
	, coalesce(tr.tokens_sent_count, 0) as tokens_sent_count
	, coalesce(tr.tokens_sent_tx_count, 0) as tokens_sent_tx_count
	, tr.first_transfer_block_time as first_transfer_block_time
	, tr.last_transfer_block_time as last_transfer_block_time
	, tr.first_received_block_number as first_received_block_number
	, tr.last_received_block_number as last_received_block_number
	, tr.first_sent_block_number as first_sent_block_number
	, tr.last_sent_block_number as last_sent_block_number
	, coalesce(tr.received_volume_usd, 0) as received_volume_usd
	, coalesce(tr.sent_volume_usd, 0) as sent_volume_usd
	, et.first_tx_block_time as first_tx_block_time
	, et.last_tx_block_time as last_tx_block_time
	, et.first_tx_block_number as first_tx_block_number
	, et.last_tx_block_number as last_tx_block_number
	, array_max(filter(array[et.last_tx_block_time, tr.last_transfer_block_time], x -> x is not null)) as last_seen
	, array_max(filter(array[et.last_tx_block_number, tr.last_received_block_number, tr.last_sent_block_number], x -> x is not null)) as last_seen_block
from
	transfers as tr
full outer join executed_txs as et
	on tr.address = et.address
	and tr.address_prefix = et.address_prefix
left join ffb
	on coalesce(tr.address, et.address) = ffb.address
	and coalesce(tr.address_prefix, et.address_prefix) = ffb.address_prefix
left join is_contract as ic
	on coalesce(tr.address, et.address) = ic.address
	and coalesce(tr.address_prefix, et.address_prefix) = ic.address_prefix
where
	coalesce(tr.address, et.address) is not null
{% endmacro %}
