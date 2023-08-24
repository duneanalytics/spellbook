{{ config( 
  schema = 'rubicon_arbitrum',
  alias = alias('table_name', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1