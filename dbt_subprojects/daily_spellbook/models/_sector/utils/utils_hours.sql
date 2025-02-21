{{ config(
    schema = 'utils',
    alias = 'hours',
    materialized = 'view',
    post_hook='{{ expose_spells(\'[]\',
                                    "sector",
                                    "utils",
                                    \'["0xRob"]\') }}'
    )
}}


select * from {{ref('utils_hours_table')}}
where timestamp <= now()