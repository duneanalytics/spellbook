{{ config( 
  schema = 'opx_finance_optimism',
  alias = alias('trades', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1