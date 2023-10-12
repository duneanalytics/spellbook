{{ config(
	tags=['legacy'],
    alias = alias('first_funded_by', legacy_model=True)
    )
}}

SELECT 
    1 as dummy 
