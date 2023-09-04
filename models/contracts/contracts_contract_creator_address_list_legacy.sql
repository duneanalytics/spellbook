{{ 
  config(
	tags=['legacy'],
    schema = 'contracts',
    alias = alias('contract_creator_address_list', legacy_model=True)
    )  
}}


SELECT 
  1