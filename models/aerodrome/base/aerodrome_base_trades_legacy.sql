{{ config(
	tags=['legacy'],
	schema = 'aerodrome_base',
    alias = alias('trades', legacy_model=True)
    )
}}

SELECT 1