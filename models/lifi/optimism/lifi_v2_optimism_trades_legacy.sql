{{ config(
	tags=['legacy'],
	
    schema = 'lifi_v2_optimism',
    alias = alias('trades', legacy_model=True)
    )
}}

SELECT 
1 as dummy 