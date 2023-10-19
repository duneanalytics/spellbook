{{ 
    config(
        schema = 'camelot_v3_arbitrum',
        alias = alias('trades', legacy_model=True),
        tags=['legacy']
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select
    1
