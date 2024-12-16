{{ config(
        alias = 'data',
        schema = 'some_test',
        materialized = 'table'
        )
}}

select * from
FROM {{ source("resident_wizards", "dataset_uploaded_data", database = "dune") }}
