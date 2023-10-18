{{
    config(
        tags = ['legacy'],
        schema = 'addresses_events_celo',
	    alias = alias('first_activity', legacy_model=True)
    )
}}

select 1
