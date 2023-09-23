{{ config( 
  schema = 'keep3r_network',
<<<<<<<< HEAD:models/keep3r_network/keep3r_network_job_migration_legacy.sql
  alias = alias('etv_job_migration', legacy_model=True),
========
  alias = alias('etv_liquidity_addition', legacy_model=True),
>>>>>>>> 2c873279 (feat: add liquidity addition and withdral queries):models/keep3r_network/keep3r_network_liquidity_addition_legacy.sql
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  888