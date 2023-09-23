{{ config( 
  schema = 'keep3r_network',
<<<<<<<< HEAD:models/keep3r_network/keep3r_network_keeper_work_legacy.sql
  alias = alias('etv_keeper_work', legacy_model=True),
========
  alias = alias('etv_liquidity_withdrawal', legacy_model=True),
>>>>>>>> 2c873279 (feat: add liquidity addition and withdral queries):models/keep3r_network/keep3r_network_liquidity_withdrawal_legacy.sql
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  888