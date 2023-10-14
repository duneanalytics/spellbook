{{ config(
        schema='prices_zksync',
        alias = alias('tokens', legacy_model=True),
        materialized='table',
        file_format = 'delta',
        tags=['legacy', 'static']
        )
}}
SELECT 1