{{ config(
	schema='tokens_cronos',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	unique_key=['block_date', 'unique_key'],
	merge_skip_unchanged=true,
) }}

-- cronos has no erc4626_cronos source, so the erc4626 union present in morph's
-- base_transfers is intentionally dropped (mirrors L1 chains without vault sources).

{{ transfers_base(
	blockchain='cronos',
	traces=source('cronos', 'traces'),
	transactions=source('cronos', 'transactions'),
	erc20_transfers=source('erc20_cronos', 'evt_Transfer'),
) }}
