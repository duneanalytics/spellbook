{{ 
  config(
	tags=['legacy'],
	
    alias = alias('contract_overrides', legacy_model=True)
    ) 
}}

select 
  1 as contract_address
  ,1 AS contract_project
  ,1 as contract_name
