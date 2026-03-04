{{ config(
    schema = 'dex_blast'
    , alias = 'token_volumes_daily'
    , partition_by = ['block_month']
    , materialized = 'table'
    , file_format = 'delta'
    , tags = ['static']
    )
}}


WITH daily_token_volumes AS (
    {{
        dex_token_volumes_daily(
            blockchain = 'blast'
            , dev_dates = var('dev_dates', false)
        )
    }}
)

SELECT *
FROM daily_token_volumes
