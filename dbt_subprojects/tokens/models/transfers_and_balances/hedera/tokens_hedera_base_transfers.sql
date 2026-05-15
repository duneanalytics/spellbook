{{ config(
	schema='tokens_hedera',
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
	blockchain='hedera',
	traces=source('hedera', 'traces'),
	transactions=source('hedera', 'transactions'),
	erc20_transfers=source('erc20_hedera', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='hedera',
			transactions=source('hedera', 'transactions'),
			erc20_transfers=source('erc20_hedera', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_hedera', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_hedera', 'evt_withdraw'),
		) }}
	)

{% raw %}
-- TODO: uncomment once WHBAR (0x0000000000000000000000000000000000163b5a) is
-- submitted to Dune's decoder under schema `whbar_hedera`. The deployed
-- contract emits Deposit/Withdrawal with two indexed addresses (src, dst, wad);
-- transfers_base_wrapped_token only reads dst from Deposit and src from
-- Withdrawal, so the existing macro works as-is. Backfill will take time after
-- the contract is decoded — uncomment in a follow-up PR. See CUR2-493.
--
-- union all
--
-- select
-- 	*
-- from
-- 	(
-- 		{{ transfers_base_wrapped_token(
-- 			blockchain='hedera',
-- 			transactions=source('hedera', 'transactions'),
-- 			wrapped_token_deposit=source('whbar_hedera', 'whbar_evt_deposit'),
-- 			wrapped_token_withdrawal=source('whbar_hedera', 'whbar_evt_withdrawal'),
-- 		) }}
-- 	)
{% endraw %}
