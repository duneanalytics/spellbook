{{ config(
    schema = 'opensea_v3_ethereum',
    alias = alias('base_trades', legacy_model=True),
    tags = ['legacy']
    )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1