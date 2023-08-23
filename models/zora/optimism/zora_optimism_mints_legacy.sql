{{ config(
    schema = 'zora_optimism',
	tags=['legacy'],
    alias = alias('mints', legacy_model=True)
)
}}

SELECT 1