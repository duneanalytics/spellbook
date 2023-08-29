{{ 
  config(
	tags=['legacy'],
    schema = 'contracts',
    alias = alias('contract_overrides', legacy_model=True)
    ) 
}}

select 
  1