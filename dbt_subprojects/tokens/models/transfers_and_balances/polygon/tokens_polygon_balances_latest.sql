{{config(
    schema = 'tokens_polygon',
    alias = 'balances_latest',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_key', 'block_date_latest'],
    partition_by = ['block_date_latest'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "sector",
                                "balances_latest",
                                \'["et-dynamic"]\') }}'
)
}}

{{
    balances_latest(
        balances = ref('tokens_polygon_balances'),
    )
}}
