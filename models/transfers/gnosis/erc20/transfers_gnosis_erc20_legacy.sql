{{ config(
        tags = ['legacy'],
        schema = 'transfers_gnosis',
        alias = alias('erc20', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 