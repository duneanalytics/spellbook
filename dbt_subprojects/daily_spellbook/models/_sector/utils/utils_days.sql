{{ config(
    schema = 'utils',
    alias = 'days',
    materialized = 'view',
    post_hook='{{ expose_spells(\'[]\',
                                    "sector",
                                    "utils") }}'
    )
}}


select * from {{ref('utils_days_table')}}
where timestamp <= now()