{{ config( 
  schema = 'op_governance_optimism',
  alias = alias('votingPower_incremental', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  

select 
  1