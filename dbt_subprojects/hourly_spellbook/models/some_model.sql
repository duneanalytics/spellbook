{{ config(
        alias = 'model',
        tags = ['prod_exclude'],
        schema = 'some_test',
        materialized = 'table'
        )
}}

show tables from dune.resident_wizards
