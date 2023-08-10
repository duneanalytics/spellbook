{{ config(
	tags=['legacy'],
	
	schema = 'synthetix_v1_optimism',
	alias = alias('perpetual_trades', legacy_model=True)
	)
}}

SELECT 
	1 as dummy 