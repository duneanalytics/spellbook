{{ config(
	schema='tokens_polygon',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	unique_key=['block_date', 'unique_key'],
	merge_skip_unchanged=true,
) }}

{{ transfers_base(
	blockchain='polygon',
	traces=source('polygon', 'traces'),
	transactions=source('polygon', 'transactions'),
	erc20_transfers=source('erc20_polygon', 'evt_Transfer'),
	native_source=ref('tokens_polygon_transfers_from_traces_base'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='polygon',
			transactions=source('polygon', 'transactions'),
			wrapped_token_deposit=source('mahadao_polygon', 'wmatic_evt_deposit'),
			wrapped_token_withdrawal=source('mahadao_polygon', 'wmatic_evt_withdrawal'),
		) }}
	)

union all

select
	{{ dbt_utils.generate_surrogate_key(['tr.block_number', 'tx.index', 'cast(null as bigint)', "array_join(tr.trace_address, ',')"]) }} as unique_key
	, 'polygon' as blockchain
	, cast(date_trunc('month', tr.block_date) as date) as block_month
	, tr.block_date
	, tr.block_time
	, tr.block_number
	, tr.tx_hash
	, cast(null as bigint) as evt_index
	, tr.trace_address
	, 'native' as token_standard
	, tx."from" as tx_from
	, tx."to" as tx_to
	, tx."index" as tx_index
	, tr."from"
	, tr."to" as to
	, tr.contract_address
	, tr.amount_raw
	, current_timestamp as _updated_at
from {{ ref('tokens_polygon_transfers_from_traces_base') }} as tr
inner join {{ source('polygon', 'transactions') }} as tx
	on tx.block_date = tr.block_date
	and tx.block_number = tr.block_number
	and tx.hash = tr.tx_hash
	{% if is_incremental() %}
	and {{ incremental_predicate('tx.block_time') }}
	{% endif %}
	{% if target.name == 'ci' %}
	and tx.block_time >= now() - interval '3' day
	{% endif %}
where tr.token_standard = 'native'
	and (
		tr."to" = (select token_address from {{ source('dune', 'blockchains') }} where name = 'polygon')
		or tr."from" = (select token_address from {{ source('dune', 'blockchains') }} where name = 'polygon')
	)
	and not exists (
		select 1
		from {{ source('erc20_polygon', 'evt_Transfer') }} as et
		where et.evt_block_number = tr.block_number
			and et.evt_tx_hash = tr.tx_hash
			and et.contract_address = (select token_address from {{ source('dune', 'blockchains') }} where name = 'polygon')
			{% if is_incremental() %}
			and {{ incremental_predicate('et.evt_block_time') }}
			{% endif %}
			{% if target.name == 'ci' %}
			and et.evt_block_time >= now() - interval '3' day
			{% endif %}
	)
	{% if is_incremental() %}
	and {{ incremental_predicate('tr.block_time') }}
	{% endif %}
	{% if target.name == 'ci' %}
	and tr.block_time >= now() - interval '3' day
	{% endif %}
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='polygon',
			transactions=source('polygon', 'transactions'),
			erc20_transfers=source('erc20_polygon', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_polygon', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_polygon', 'evt_withdraw'),
		) }}
	)
