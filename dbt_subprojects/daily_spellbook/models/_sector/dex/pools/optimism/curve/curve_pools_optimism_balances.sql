{{
  config(
    schema = 'curve_pools_optimism',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_address', 'snapshot_day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.snapshot_day')]
    )
}}

WITH op_pools AS (
    SELECT DISTINCT
        CAST(pool AS varchar(42)) as pool_address,
        tokenid,
        token
    FROM {{ source('curve_optimism', 'pools') }}
    WHERE CAST(token AS varchar(42)) = '0x4200000000000000000000000000000000000042'
)

SELECT 
    p.pool_address,
    p.tokenid,
    p.token,
    b.balance AS op_balance,
    b.day AS snapshot_day
FROM op_pools p
JOIN {{ source('tokens_optimism', 'balances_daily') }} b
    ON p.pool_address = CAST(b.address AS varchar(42))
WHERE CAST(b.token_address AS varchar(42)) = '0x4200000000000000000000000000000000000042'
{% if is_incremental() %}
    AND b.day >= current_date - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}
{% endif %}