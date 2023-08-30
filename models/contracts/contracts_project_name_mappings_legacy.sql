{{ 
  config(
	tags=['legacy'],
    schema = 'contracts',
    alias = alias('project_name_mappings', legacy_model=True)
    )  
}}

select 
  1