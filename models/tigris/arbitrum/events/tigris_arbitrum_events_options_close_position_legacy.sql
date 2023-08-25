{{ config(
	tags=['legacy'],
    schema = 'tigris_arbitrum',
    alias = alias('options_close_position', legacy_model=True)
    )
}}

SELECT 
    1 as dummy