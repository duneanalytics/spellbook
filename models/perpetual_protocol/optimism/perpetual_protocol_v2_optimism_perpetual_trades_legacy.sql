{{ config(
	tags=['legacy'],
	
	schema = 'perpetual_protocol_v2_optimism',
	alias = alias('perpetual_trades', legacy_model=True)
	)
}}

SELECT
1 as dummy  
