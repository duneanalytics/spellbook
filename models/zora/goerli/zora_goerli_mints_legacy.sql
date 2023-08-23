{{ config(
    schema = 'zora_goerli',
	tags=['legacy'],
    alias = alias('mints', legacy_model=True)
)
}}

SELECT 1