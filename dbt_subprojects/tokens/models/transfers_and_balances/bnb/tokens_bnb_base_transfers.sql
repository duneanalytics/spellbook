{{ config(
	schema='tokens_bnb',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	incremental_predicates=[
		incremental_month_predicate('DBT_INTERNAL_DEST.block_month'),
		incremental_predicate('DBT_INTERNAL_DEST.block_time'),
	],
	unique_key=['block_date', 'unique_key'],
	merge_skip_unchanged=true,
) }}

-- CI-only scan bound (target=ci); prod/full-refresh unaffected.
{% if target.name == 'ci' -%}
select * from (
{%- endif %}

{{ transfers_base(
	blockchain='bnb',
	traces=source('bnb', 'traces'),
	transactions=source('bnb', 'transactions'),
	erc20_transfers=source('erc20_bnb', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='bnb',
			transactions=source('bnb', 'transactions'),
			wrapped_token_deposit=source('bnb_bnb', 'WBNB_evt_Deposit'),
			wrapped_token_withdrawal=source('bnb_bnb', 'WBNB_evt_Withdrawal'),
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='bnb',
			transactions=source('bnb', 'transactions'),
			erc20_transfers=source('erc20_bnb', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_bnb', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_bnb', 'evt_withdraw'),
		) }}
	)

{% if target.name == 'ci' -%}
) as _ci_bounded
where block_time >= now() - interval '3' day
{%- endif %}
