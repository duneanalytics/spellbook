{{ config(
        schema='dune_temp',
        alias = 'dummy_view',
        materialized='view'
        )
}}

select 1 as test_column