{{ 
  config(
	tags=['legacy'],
	
    alias = alias('project_name_mappings', legacy_model=True)
    )  
}}

select 
  1 as dune_name
  ,1 as mapped_name