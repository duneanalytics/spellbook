{{ config(
    schema = 'utils',
    alias = 'months',
    materialized = 'view',
    post_hook='{{ expose_spells(\'[]\',
                                    "sector",
                                    "utils",
                                    \'["0xRob", "hildobby"]\') }}'
    )
}}


select * from {{ref('utils_months_table')}}
where timestamp <= now()