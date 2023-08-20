{{ config( 
  alias = alias('traces', legacy_model=True),
  tags = ['legacy', 'prod_exclude']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1