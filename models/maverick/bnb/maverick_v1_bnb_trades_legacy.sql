{{ config(
    tags=['legacy'],
    schema = 'maverick_v1_bnb',
    alias = alias('trades', legacy_model=True)
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1