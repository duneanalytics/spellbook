{{ config(
        alias = 'data',
        schema = 'some_test'
        )
}}

select * from
FROM {{ source("resident_wizards", "dataset_uploaded_data", database = "dune") }}
