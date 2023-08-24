{{ config(
	    tags=['legacy'],
    schema = 'tigris_polygon',
    alias = alias('events_modify_margin', legacy_model=True)
    )
}}

SELECT 
     1 as dummy