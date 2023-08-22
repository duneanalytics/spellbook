{{ config(
    tags=['legacy'],
    alias = alias('price_feeds', legacy_model=True)
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select 
    1