{{ config(
    schema = 'base_ethereum',
	tags=['legacy'],
    alias = alias('mints', legacy_model=True)
)
}}

SELECT 1