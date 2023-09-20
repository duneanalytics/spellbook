{{ config(
        tags = ['legacy'],
        schema = 'transfers_arbitrum_erc20',
        alias = alias('agg_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 