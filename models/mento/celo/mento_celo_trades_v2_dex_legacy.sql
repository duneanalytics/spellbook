{{ 
    config(
        tags = ['legacy'],
        schema = 'mento_celo',
        alias = alias('trades_v2_dex', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
