{{ config( 
  schema = 'op_governance_optimism',
  alias = alias('delegates', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1