{{ 
    config(
        schema = 'uniswap_v3_celo',
        alias = alias('flashloans', legacy_model=True),
        tags = ['legacy']
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
