{{ config(
    tags=['legacy'],
        alias = alias('events_contracts_positions', legacy_model=True)
        )
}}

SELECT 
    1 as dummy