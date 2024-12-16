{{ config(
        alias = 'data',
        schema = 'some_test',
        materialized = 'table'
        )
}}

select * FROM {{ source("resident_wizards", "dataset_uploaded_data", database = "dune") }}
