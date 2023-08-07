{{ 
  config(
	tags=['legacy'],
	
    alias = alias('contract_overrides', legacy_model=True)
    ) 
}}

select 
  1