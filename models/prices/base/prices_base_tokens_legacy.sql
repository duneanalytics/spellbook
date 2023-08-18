{{ 
    config(
        schema = 'prices_base',
        alias = alias('tokens', legacy_model=True),
        tags=['legacy']
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1