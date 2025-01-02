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

SELECT 
    CAST(p.pool AS varchar(42)) as pool_address,
    p.tokenid,
    p.token,
    b.balance AS op_balance,
    b.day AS snapshot_day
FROM 
    {{ source('curve_optimism', 'pools') }} p
JOIN 
    {{ source('tokens_optimism', 'balances_daily') }} b
ON 
    CAST(p.pool AS varchar(42)) = CAST(b.address AS varchar(42))
WHERE 
    CAST(p.token AS varchar(42)) = '0x4200000000000000000000000000000000000042'
    AND CAST(b.token_address AS varchar(42)) = '0x4200000000000000000000000000000000000042'