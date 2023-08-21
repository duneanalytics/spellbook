{{ config( 
  schema = 'schema_name',
  alias = alias('table_name', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1