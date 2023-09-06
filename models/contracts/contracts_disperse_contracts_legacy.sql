{{ config(
	tags=['legacy'],
	schema = 'contracts',
    alias = alias('disperse_contracts', legacy_model=True)
    )
}}

SELECT
    1