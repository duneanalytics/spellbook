{{ 
    config(
        schema = 'trader_joe_v2_1_bnb',
        alias = alias('trades', legacy_model=True),
        tags=['legacy']
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1