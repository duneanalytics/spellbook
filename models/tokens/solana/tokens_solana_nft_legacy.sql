{{ config(
    schema = 'tokens_solana_nft',
    tags = ['legacy'],
    alias = alias('nft', legacy_model=True),
)
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1