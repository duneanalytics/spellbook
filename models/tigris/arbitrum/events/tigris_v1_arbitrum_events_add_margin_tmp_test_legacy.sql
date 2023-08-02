{{ config(
    tags=['legacy'],
    schema = 'tigris_v1_arbitrum',
    alias = alias('events_add_margin_tmp_test', legacy_model=True )
    )
}}

SELECT 
    1