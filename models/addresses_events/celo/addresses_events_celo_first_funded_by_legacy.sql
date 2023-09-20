{{ config(
    schema = 'addresses_events_celo',
	tags=['legacy'],
    alias = alias('first_funded_by', legacy_model=True)
    )
}}

SELECT 
    1 as dummy 