SELECT 
    p.pool,
    p.tokenid,
    p.token,
    b.balance AS op_balance,
    b.day AS snapshot_day
FROM 
    {{ source('curve_optimism', 'pools') }} p
JOIN 
    {{ source('tokens_optimism', 'balances_daily') }} b
ON 
    p.pool = b.address
WHERE 
    p.token = '0x4200000000000000000000000000000000000042'
    AND b.token_address = '0x4200000000000000000000000000000000000042';
