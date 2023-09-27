{{
    config(
        tags = ['legacy'],
        schema = 'sushiswap_celo',
        alias = alias('trades', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
