{{ config(
	tags=['legacy'],
    alias = alias('first_path', legacy_model=True)
    )
}}

SELECT 
    1 as dummy 
