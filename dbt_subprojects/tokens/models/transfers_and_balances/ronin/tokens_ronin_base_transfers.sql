{{ config(
	schema='tokens_ronin',
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
	blockchain='ronin',
	traces=source('ronin', 'traces'),
	transactions=source('ronin', 'transactions'),
	erc20_transfers=source('erc20_ronin', 'evt_Transfer'),
) }}

union all

-- WRON uses non-standard ABI column names (sender/value instead of dst/src/wad)
select
	*
from
	(
		{% set wron_deposit %}
		(select *, sender as dst, value as wad from {{ source('wron_ronin', 'wron_evt_deposit') }})
		{% endset %}
		{% set wron_withdrawal %}
		(select *, sender as src, value as wad from {{ source('wron_ronin', 'wron_evt_withdrawal') }})
		{% endset %}
		{{ transfers_base_wrapped_token(
			blockchain='ronin',
			transactions=source('ronin', 'transactions'),
			wrapped_token_deposit=wron_deposit,
			wrapped_token_withdrawal=wron_withdrawal,
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='ronin',
			transactions=source('ronin', 'transactions'),
			erc20_transfers=source('erc20_ronin', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_ronin', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_ronin', 'evt_withdraw'),
		) }}
	)
