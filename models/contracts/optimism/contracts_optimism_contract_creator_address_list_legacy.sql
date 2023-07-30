{{ 
  config(
	tags=['legacy'],
	
    alias = alias('contract_creator_address_list', legacy_model=True),
    unique_key='creator_address',
    )  
}}


SELECT 1 as creator_address, 1 as contract_project