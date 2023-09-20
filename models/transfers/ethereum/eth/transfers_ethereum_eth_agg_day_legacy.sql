{{ config(
        tags = ['legacy'],
        schema = 'transfers_ethereum_eth',
        alias = alias('eth_agg_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 