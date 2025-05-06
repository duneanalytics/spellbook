{{ config(
    schema = 'utils',
    alias = 'minutes',
    materialized = 'view',
    post_hook='{{ expose_spells(\'[]\',
                                    "sector",
                                    "utils",
                                    \'["0xRob"]\') }}'
    )
}}


select * from {{ref('utils_minutes_table')}}
where timestamp <= now()