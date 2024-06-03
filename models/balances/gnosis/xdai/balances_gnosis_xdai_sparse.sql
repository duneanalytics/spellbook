{{ config(
        
        alias = 'xdai_sparse',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "balances",
                                    \'["hdser"]\') }}'
        )
}}

WITH 

transfers_gnosis_xdai AS (
    SELECT
        *
    FROM
        {{ ref('transfers_gnosis_xdai') }}
),


suicide_end_balances as (
    SELECT
        blockchain, 
        block_time,
        block_number,
        block_month,
        wallet_address, 
        refund_address,
        token_address, 
        amount_raw 
            - 
        COALESCE(LAG(amount_raw) OVER (PARTITION BY blockchain, token_address, wallet_address ORDER BY block_time),0) AS amount_raw
    FROM (
        SELECT
            t1.blockchain, 
            t2.block_time,
            t2.block_number,
            t2.block_month,
            t1.wallet_address, 
            t2.refund_address,
            t1.token_address, 
            SUM(t1.amount_raw) OVER (PARTITION BY t1.blockchain, t1.token_address, t1.wallet_address, t2.tx_hash ORDER BY t1.block_time) AS amount_raw,
            ROW_NUMBER() OVER (PARTITION BY t1.blockchain, t1.token_address, t1.wallet_address, t2.tx_hash ORDER BY t1.block_time DESC) AS row_cnt
        FROM 
            transfers_gnosis_xdai t1
        INNER JOIN  
            (SELECT * FROM {{ ref('suicide_gnosis_xdai') }}) t2
            ON 
            t2.address = t1.wallet_address
        WHERE
        t1.block_number <= t2.block_number
    )
    WHERE row_cnt = 1
),


suicide_balances_diff AS (
    SELECT
        blockchain, 
        block_time,
        block_number,
        block_month,
        wallet_address,
        token_address, 
        -amount_raw AS amount_raw
    FROM 
        suicide_end_balances

    UNION ALL

    SELECT
        blockchain, 
        block_time,
        block_number,
        block_month,
        refund_address AS wallet_address,
        token_address, 
        amount_raw
    FROM 
        suicide_end_balances
    WHERE
        refund_address != wallet_address
),

transfers_gnosis_xdai_full AS (
    SELECT 
        blockchain, 
        block_time,
        block_number,
        block_month,
        wallet_address,
        token_address, 
        amount_raw
    FROM    
        transfers_gnosis_xdai
    
    UNION ALL

    SELECT 
        blockchain, 
        block_time,
        block_number,
        block_month,
        wallet_address,
        token_address, 
        amount_raw
    FROM 
        suicide_balances_diff
)


 SELECT
        blockchain, 
        block_time,
        block_number,
        block_month,
        wallet_address, 
        token_address, 
        amount_raw,
        amount_raw - COALESCE(LAG(amount_raw) OVER (PARTITION BY wallet_address ORDER BY block_time),0 ) AS amount_raw_diff
    FROM (
        SELECT
            blockchain, 
            block_time,
            block_number,
            block_month,
            wallet_address, 
            token_address, 
            SUM(amount_raw) OVER (PARTITION BY blockchain, token_address, wallet_address ORDER BY block_time) AS amount_raw,
            ROW_NUMBER() OVER (PARTITION BY blockchain, token_address, wallet_address, block_number ) AS row_cnt
        FROM 
            transfers_gnosis_xdai_full
    )
    WHERE row_cnt = 1

