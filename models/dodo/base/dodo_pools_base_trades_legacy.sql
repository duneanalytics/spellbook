{{ 
    config(
        alias = alias('pool_trades', legacy_model=True),
        tags=['legacy']
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select
    1