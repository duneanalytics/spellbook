{{ config(
    schema = 'utils',
    alias = 'months',
    materialized = 'view',
    post_hook='{{ expose_spells(\'[]\',
                                    "sector",
                                    "utils") }}'
    )
}}


select * from {{ref('utils_months_table')}}
where timestamp <= now()