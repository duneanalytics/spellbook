{{ config(
    tags=['legacy'],
    schema = 'tigris_arbitrum',
    alias = alias('events_add_margin', legacy_model=True )
    )
}}

SELECT 
    1 as dummy