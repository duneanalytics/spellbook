{{ config(
	tags=['legacy'],
	schema = 'nft_base',
        alias = alias('transfers', legacy_model=True)
)
}}

 SELECT 1 as blockchain
