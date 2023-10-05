{{
    config(
        tags = ['legacy'],
        schema = 'ubeswap_celo',
        alias = alias('trades', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
