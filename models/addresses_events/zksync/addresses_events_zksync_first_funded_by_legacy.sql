{{ config(
	tags=['legacy'],
	schema = 'addresses_events_zksync',
        alias = alias('first_funded_by', legacy_model=True)
    )
}}

SELECT 
    1 as dummy 
