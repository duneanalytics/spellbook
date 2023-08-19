{{ config(
	tags=['legacy'],
	
        alias = alias('mainet', legacy_model=True)
        )
}}

SELECT 
    1 as dummmy