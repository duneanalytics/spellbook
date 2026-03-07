{{ config(
        schema='dune_temp',
        alias = 'dummy_view',
        materialized='view',
        tags = ['prod_exclude']
        )
}}

select 1 as test_column