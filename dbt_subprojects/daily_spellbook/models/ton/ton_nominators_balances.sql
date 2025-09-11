{{ config(
        schema = 'ton_nominators'
        , alias = 'balances'
        , materialized = 'view'
    )
}}

SELECT pool, user_address, pool_type, greatest(0, sum(value * case when direction = 'in' then 1 else -1 end) )/ 1e9 as current_balance,
min_by(tx_hash, block_time) as first_tx,
max_by(tx_hash, block_time) as last_tx,
count(1) as tx_count
FROM  {{ ref('ton_nominators_cashflow') }}
GROUP BY 1, 2, 3
ORDER BY 4 DESC