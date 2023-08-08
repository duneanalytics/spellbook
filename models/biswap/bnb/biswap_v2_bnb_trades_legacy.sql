{{ config(
    tags=['legacy'],
    schema = 'biswap_v2_bnb',
    alias = alias('trades', legacy_model=True)
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1