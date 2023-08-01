{{ config(
    tags=['legacy'],
    schema = 'carbon_defi_ethereum',
    alias = alias('trades', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
SELECT
    1
    
