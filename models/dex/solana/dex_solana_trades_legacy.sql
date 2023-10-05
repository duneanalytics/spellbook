{{ config( 
  schema = 'dex_solana',
  alias = alias('trades', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1