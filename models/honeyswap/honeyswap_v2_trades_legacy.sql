{{ config( 
  schema = 'honeyswap_v2_trades',
  alias = alias('trades', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  888