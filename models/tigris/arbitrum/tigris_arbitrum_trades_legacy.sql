{{ config(
	tags=['legacy'],
	
        alias = alias('trades', legacy_model=True)
        )
}}

SELECT 
    1 as dummmy