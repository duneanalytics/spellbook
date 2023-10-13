{{ config( 
  schema = 'op_governance_optimism',
  alias = alias('delegate_addresses', legacy_model=True),
  tags = ['legacy']
  )
}}
  

select 
  1