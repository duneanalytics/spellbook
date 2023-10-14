{{ config(
	tags=['legacy']
        , schema = 'tokens_zksync'
        , alias = alias('nft', legacy_model=True)
        )
}}

SELECT
    1