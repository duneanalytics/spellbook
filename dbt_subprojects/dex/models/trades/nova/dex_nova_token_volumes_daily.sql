{{ config(
    schema = 'dex_nova'
    , alias = 'token_volumes_daily'
    , partition_by = ['block_month']
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    )
}}


WITH daily_token_volumes AS (
    {{
        dex_token_volumes_daily(
            blockchain = 'nova'
            , dev_dates = var('dev_dates', false)
        )
    }}
)

SELECT *
FROM daily_token_volumes
