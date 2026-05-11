{{ config(
	schema='gmx_v2_avalanche_c',
	alias='glv_created',
	materialized='incremental',
	unique_key=['block_date', 'tx_hash', 'index'],
	incremental_strategy='merge',
) }}

{%- set event_name = 'GlvCreated' -%}
{%- set blockchain_name = 'avalanche_c' -%}

-- evt_tx_from / evt_tx_to replace add_tx_columns → transactions join (same pattern as market_created).
-- Single-pass unnest avoids repeated evt_data scans and parsed self-join.
with evt_data as (
	select
		'{{ blockchain_name }}' as blockchain
		, evt_block_time as block_time
		, date(evt_block_time) as block_date
		, evt_block_number as block_number
		, evt_tx_hash as tx_hash
		, evt_index as index
		, evt_tx_from as tx_from
		, evt_tx_to as tx_to
		, contract_address
		, eventName as event_name
		, eventData as data
		, msgSender as msg_sender
	from {{ source('gmx_v2_avalanche_c', 'EventEmitter_evt_EventLog1') }}
	where eventName = '{{ event_name }}'
	{% if is_incremental() -%}
	and {{ incremental_predicate('evt_block_time') }}
	{% endif %}

	union all

	select
		'{{ blockchain_name }}' as blockchain
		, evt_block_time as block_time
		, date(evt_block_time) as block_date
		, evt_block_number as block_number
		, evt_tx_hash as tx_hash
		, evt_index as index
		, evt_tx_from as tx_from
		, evt_tx_to as tx_to
		, contract_address
		, eventName as event_name
		, eventData as data
		, msgSender as msg_sender
	from {{ source('gmx_v2_avalanche_c', 'EventEmitter_evt_EventLog2') }}
	where eventName = '{{ event_name }}'
	{% if is_incremental() -%}
	and {{ incremental_predicate('evt_block_time') }}
	{% endif %}
)
, items_unnested as (
	select
		ed.blockchain
		, ed.block_time
		, ed.block_date
		, ed.block_number
		, ed.tx_hash
		, ed.index
		, ed.tx_from
		, ed.tx_to
		, ed.contract_address
		, ed.event_name
		, ed.msg_sender
		, json_extract_scalar(cast(item as varchar), '$.key') as key_name
		, json_extract_scalar(cast(item as varchar), '$.value') as value
	from evt_data as ed
	cross join unnest(
		coalesce(
			cast(json_extract(json_query(ed.data, 'lax $.addressItems' OMIT QUOTES), '$.items') as array(json))
			, cast(array[] as array(json))
		)
		|| coalesce(
			cast(json_extract(json_query(ed.data, 'lax $.bytes32Items' OMIT QUOTES), '$.items') as array(json))
			, cast(array[] as array(json))
		)
	) as t(item)
)
, full_data as (
	select
		blockchain
		, block_time
		, block_date
		, block_number
		, tx_hash
		, index
		, max(tx_from) as tx_from
		, max(tx_to) as tx_to
		, max(contract_address) as contract_address
		, max(event_name) as event_name
		, max(msg_sender) as msg_sender
		, from_hex(max(case when key_name = 'glvToken' then value end)) as glv_token
		, from_hex(max(case when key_name = 'longToken' then value end)) as long_token
		, from_hex(max(case when key_name = 'shortToken' then value end)) as short_token
		, from_hex(max(case when key_name = 'salt' then value end)) as salt
		, from_hex(max(case when key_name = 'glvType' then value end)) as glv_type
		, 'GM' as market_token_symbol
		, 18 as market_token_decimals
	from items_unnested
	group by
		blockchain
		, block_time
		, block_date
		, block_number
		, tx_hash
		, index
)

select
	fd.blockchain
	, fd.block_time
	, fd.block_date
	, fd.block_number
	, fd.tx_hash
	, fd.index
	, fd.contract_address
	, fd.event_name
	, fd.msg_sender
	, fd.glv_token
	, fd.long_token
	, fd.short_token
	, fd.salt
	, fd.glv_type
	, fd.market_token_symbol
	, fd.market_token_decimals
	, fd.tx_from
	, fd.tx_to
from full_data as fd
