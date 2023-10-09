{{ config( 
  schema = 'op_governance_optimism',
  alias = alias('delegators_incremental', legacy_model=True),
  tags = ['legacy']
  )
}}
  
select 
  1