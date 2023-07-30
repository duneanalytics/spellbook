{{ 
  config(
	tags=['legacy'],
	
    alias = alias('contract_creator_address_list', legacy_model=True)
    )  
}}


SELECT 
  1 as creator_address, 1 as contract_project