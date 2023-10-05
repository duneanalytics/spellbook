{{ config( 
  schema = 'keep3r_network',
  alias = alias('etv_job_migration', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  888