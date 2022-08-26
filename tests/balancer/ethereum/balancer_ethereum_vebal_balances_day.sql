SELECT 
    day,
    wallet_address,
    bpt_balance,
    vebal_balance,
    lock_time
FROM {{ ref('balancer_ethereum_vebal_balances_day') }}
WHERE day = '2022-08-25'

EXCEPT

SELECT 
    day,
    provider,
    bpt_balance,
    vebal,
    lock_period
FROM {{ ref('balancer_ethereum_vebal_balances_day_20220825') }}

