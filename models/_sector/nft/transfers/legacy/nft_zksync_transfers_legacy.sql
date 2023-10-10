{{ config(
	tags=['legacy'],
	schema = 'nft_zksync',
        alias = alias('transfers', legacy_model=True)
)
}}

 SELECT 1