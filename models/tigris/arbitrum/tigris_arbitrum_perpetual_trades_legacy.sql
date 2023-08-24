{{ config(
	tags=['legacy'],
	
        alias = alias('perpetual_trades', legacy_model=True)
        )
}}

SELECT 
    1 as dummmy