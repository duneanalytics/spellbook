{{ config(
    schema = 'utils',
    alias = 'years',
    materialized = 'view',
    post_hook='{{ expose_spells(\'[]\',
                                    "sector",
                                    "utils") }}'
    )
}}


select * from {{ref('utils_years_table')}}
where timestamp <= now()