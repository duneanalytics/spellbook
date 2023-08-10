{{ config(
	tags=['legacy'],
	
	schema = 'pika_v3_optimism',
	alias = alias('perpetual_trades', legacy_model=True)
	)
}}

SELECT 
	1 as dummy 