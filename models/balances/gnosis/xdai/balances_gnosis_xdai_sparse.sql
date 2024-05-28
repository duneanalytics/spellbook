{{ config(
        
        alias = 'xdai_sparse',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "balances",
                                    \'["hdser"]\') }}'
        )
}}

WITH 

sparse_balances as (
    SELECT
        blockchain, 
        block_time,
        block_number,
        block_month,
        wallet_address, 
        token_address, 
        amount_raw
    FROM (
        SELECT
            blockchain, 
            block_time,
            block_number,
            block_month,
            wallet_address, 
            token_address, 
            SUM(amount_raw) OVER (PARTITION BY blockchain, token_address, wallet_address ORDER BY block_time) AS amount_raw,
            ROW_NUMBER() OVER (PARTITION BY blockchain, token_address, wallet_address, block_time ORDER BY trace_address) AS row_cnt
        FROM 
        {{ ref('transfers_gnosis_xdai') }}
    )
    WHERE row_cnt = 1
),

suicide as (
    SELECT
         block_time
         ,block_number
        ,address
        ,refund_address
    FROM 
    {{ ref('suicide_gnosis_xdai') }}
),

suicide_balances as (
    SELECT 
        blockchain, 
        block_time,
        block_number,
        block_month,
        wallet_address, 
        token_address, 
        SUM(amount_raw) AS amount_raw
    FROM (
        SELECT
            t1.blockchain, 
            t1.block_time,
            t1.block_number,
            t1.block_month,
            t2.address AS wallet_address, 
            t1.token_address, 
            -t1.amount_raw AS amount_raw
        FROM
            sparse_balances t1
        INNER JOIN
            suicide t2
            ON
            t2.block_number = t1.block_number
            
        
        UNION ALL

        SELECT
            t1.blockchain, 
            t1.block_time,
            t1.block_number,
            t1.block_month,
            t2.refund_address AS wallet_address, 
            t1.token_address, 
            t1.amount_raw AS amount_raw
        FROM
            sparse_balances t1
        INNER JOIN
            suicide t2
            ON
            t2.block_number = t1.block_number
    )
    GROUP BY 
        1,2,3,4,5,6
)


SELECT
    blockchain, 
    block_time,
    block_number,
    block_month,
    wallet_address, 
    token_address, 
    SUM(amount_raw) AS amount_raw
FROM (
    SELECT * FROM sparse_balances
    UNION ALL
    SELECT * FROM suicide_balances
)
GROUP BY 
    1,2,3,4,5,6


