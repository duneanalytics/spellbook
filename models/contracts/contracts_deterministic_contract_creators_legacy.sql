{{ 
  config(
	tags=['legacy'],
	
    alias = alias('deterministic_contract_creators', legacy_model=True)
    )  
}}


SELECT

1 AS creator_address, 1 AS creator_name
