{{
    config(
        tags = ['legacy'],
        schema = 'moola_celo',
        alias = alias('supply', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
