{{ 
    config(
        tags = ['legacy'],
        schema = 'mento_celo',
        alias = alias('pools_v1', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
