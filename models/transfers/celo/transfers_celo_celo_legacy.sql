{{ 
    config(
	    tags=['legacy'],
        schema = 'transfers_celo',
        alias = alias('celo', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1