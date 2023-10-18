{{ config( 
  schema = 'op_governance_optimism',
  alias = alias('voting_power_incremental', legacy_model=True),
  tags = ['legacy']
  )
}}
  
select 
  1