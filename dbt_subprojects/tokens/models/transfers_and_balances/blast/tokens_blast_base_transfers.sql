{{ config(
	schema='tokens_blast',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='table',
	file_format='delta',
	tags=['static'],
	post_hook='{{ hide_spells() }}',
) }}

{{ transfers_base(
	blockchain='blast',
	traces=source('blast', 'traces'),
	transactions=source('blast', 'transactions'),
	erc20_transfers=source('erc20_blast', 'evt_Transfer'),
) }}
