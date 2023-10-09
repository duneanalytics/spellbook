{{ config(
	tags=['legacy'],
	schema = 'nft_celo',
        alias = alias('transfers', legacy_model=True)
)
}}

SELECT 1
