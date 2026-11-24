{{ config(
    schema = 'utils',
    alias = 'quarters',
    materialized = 'view',
    post_hook='{{ expose_spells(\'[]\',
                                    "sector",
                                    "utils") }}'
    )
}}


select * from {{ref('utils_quarters_table')}}
where timestamp <= now()