{{ config(
	tags=['legacy'],
    alias = alias('app_dao_addresses', legacy_model=True)
    )
}}

SELECT 
    1  as dummy 