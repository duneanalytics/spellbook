{{ config(
    tags=['legacy'],
    schema = 'tigris_v1_polygon',
    alias = alias('trades', legacy_model=True )
    )
}}

SELECT 
    1 as dummy