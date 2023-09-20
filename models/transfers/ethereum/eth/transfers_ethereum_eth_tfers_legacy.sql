{{ config(
        tags = ['legacy'],
        schema = 'transfers_ethereum_eth',
        alias = alias('eth_tfers', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 