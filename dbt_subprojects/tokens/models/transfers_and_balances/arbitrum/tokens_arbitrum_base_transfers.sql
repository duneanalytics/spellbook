{{ config(
	schema = 'tokens_arbitrum',
	alias = 'base_transfers',
	partition_by = ['block_month'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	unique_key = ['block_date', 'unique_key'],
	merge_skip_unchanged = true,
) }}

with base as (
	{{ transfers_base(
		blockchain = 'arbitrum',
		traces = source('arbitrum', 'traces'),
		transactions = source('arbitrum', 'transactions'),
		erc20_transfers = source('erc20_arbitrum', 'evt_Transfer'),
	) }}
)

{% if is_incremental() -%}
, merge_test as (
	select
		b.*
		, case
			when d.unique_key is null then 'insert'
			else 'update'
		end as reason
		, case
			when d.unique_key is null then cast(null as varchar)
			else array_join(
				filter(
					array[
						case when b.blockchain is distinct from d.blockchain then 'blockchain' end
						, case when b.block_month is distinct from d.block_month then 'block_month' end
						, case when b.block_date is distinct from d.block_date then 'block_date' end
						, case when b.block_time is distinct from d.block_time then 'block_time' end
						, case when b.block_number is distinct from d.block_number then 'block_number' end
						, case when b.tx_hash is distinct from d.tx_hash then 'tx_hash' end
						, case when b.evt_index is distinct from d.evt_index then 'evt_index' end
						, case when b.trace_address is distinct from d.trace_address then 'trace_address' end
						, case when b.token_standard is distinct from d.token_standard then 'token_standard' end
						, case when b.tx_from is distinct from d.tx_from then 'tx_from' end
						, case when b.tx_to is distinct from d.tx_to then 'tx_to' end
						, case when b.tx_index is distinct from d.tx_index then 'tx_index' end
						, case when b."from" is distinct from d."from" then 'from' end
						, case when b.to is distinct from d.to then 'to' end
						, case when b.contract_address is distinct from d.contract_address then 'contract_address' end
						, case when b.amount_raw is distinct from d.amount_raw then 'amount_raw' end
					]
					, x -> x is not null
				)
				, ','
			)
		end as columns_changed
	from
		base as b
	left join {{ this }} as d
		on d.block_date = b.block_date
		and d.unique_key = b.unique_key
)

select
	m.*
	, current_timestamp as _updated_at
from
	merge_test as m

{% else -%}

select
	base.*
	, current_timestamp as _updated_at
	, cast('insert' as varchar) as reason
	, cast(null as varchar) as columns_changed
from
	base
where
	base.block_date >= current_date - interval '30' day

{% endif -%}
