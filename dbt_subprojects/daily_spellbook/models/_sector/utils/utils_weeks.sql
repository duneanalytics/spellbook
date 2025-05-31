{{ config(
    schema = 'utils',
    alias = 'weeks',
    materialized = 'view',
    post_hook='{{ expose_spells(\'[]\',
                                    "sector",
                                    "utils",
                                    \'["0xRob", "hildobby"]\') }}'
    )
}}


select * from {{ref('utils_weeks_table')}}
where timestamp <= now()