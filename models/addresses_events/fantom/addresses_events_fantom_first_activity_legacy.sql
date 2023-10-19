{{ config(
	tags=['legacy'],
    alias = alias('first_activity', legacy_model=True)
    )
}}

SELECT 
    1 as dummy 
