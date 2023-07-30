{{ config(
	tags=['legacy'],
	
    alias = alias('disperse_contracts', legacy_model=True)
    )
}}

SELECT
    1 as contract_address, 1 as contract_name
