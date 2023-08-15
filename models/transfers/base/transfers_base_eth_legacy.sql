{{ 
    config(
	tags=['legacy'],
	
        alias = alias('eth', legacy_model=True), 
    )
}}

    select 
        1 as unique_transfer_id