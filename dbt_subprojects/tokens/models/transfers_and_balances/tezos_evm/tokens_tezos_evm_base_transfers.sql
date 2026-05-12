{{ config(
	schema='tokens_tezos_evm',
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
	blockchain='tezos_evm',
	traces=source('tezos_evm', 'traces'),
	transactions=source('tezos_evm', 'transactions'),
	erc20_transfers=source('erc20_tezos_evm', 'evt_Transfer'),
) }}
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='tezos_evm',
			transactions=source('tezos_evm', 'transactions'),
			erc20_transfers=source('erc20_tezos_evm', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_tezos_evm', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_tezos_evm', 'evt_withdraw'),
		) }}
	)
