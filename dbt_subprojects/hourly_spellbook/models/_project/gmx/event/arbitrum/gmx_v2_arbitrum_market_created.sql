{{ config(
	schema='gmx_v2_arbitrum',
	alias='market_created',
	materialized='table',
	file_format='delta',
) }}

{%- set event_name = 'MarketCreated' -%}
{%- set blockchain_name = 'arbitrum' -%}

-- evt_tx_from / evt_tx_to match transactions.from / to on all GMX MarketCreated rows (Dune MCP verify).
-- Single-pass unnest avoids repeated logs scans and evt_data self-join.
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
		, varbinary_substring(topic1, 13, 20) as account
	from {{ source('gmx_v2_arbitrum', 'EventEmitter_evt_EventLog1') }}
	where eventName = '{{ event_name }}'

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
		, varbinary_substring(topic2, 13, 20) as account
	from {{ source('gmx_v2_arbitrum', 'EventEmitter_evt_EventLog2') }}
	where eventName = '{{ event_name }}'
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
		, ed.account
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
		, max(account) as account
		, from_hex(max(case when key_name = 'marketToken' then value end)) as market_token
		, from_hex(max(case when key_name = 'indexToken' then value end)) as index_token
		, from_hex(max(case when key_name = 'longToken' then value end)) as long_token
		, from_hex(max(case when key_name = 'shortToken' then value end)) as short_token
		, from_hex(max(case when key_name = 'salt' then value end)) as salt
		, max(case when key_name = 'indexToken' then value end) = '0x0000000000000000000000000000000000000000' as spot_only
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
	fd.*
from full_data as fd
