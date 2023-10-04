{{ config(
    schema = 'tensorswap_v1_solana',
    tags = ['legacy'],
    alias = alias('trades', legacy_model=True),
)
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1