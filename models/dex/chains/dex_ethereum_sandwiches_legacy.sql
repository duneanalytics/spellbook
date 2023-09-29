{{ config(
	    tags=['legacy'],
        schema = 'dex_ethereum',
        alias = alias('sandwiches', legacy_model=True)
        )
}}

SELECT 1