{{ 
    config(
        schema = 'uniswap_celo',
        alias = alias('pools', legacy_model=True),
        tags = ['legacy']
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
