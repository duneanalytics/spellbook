{{ config(
    schema = 'dex_corn'
    , alias = 'token_volumes_daily'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'token_address', 'block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}


WITH daily_token_volumes AS (
    {{
        dex_token_volumes_daily(
            blockchain = 'corn'
            , dev_dates = var('dev_dates', false)
        )
    }}
)

SELECT *
FROM daily_token_volumes
