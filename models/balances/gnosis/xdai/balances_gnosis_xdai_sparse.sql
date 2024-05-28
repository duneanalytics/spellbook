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
        block_month,
        wallet_address, 
        token_address, 
        SUM(amount_raw) OVER (PARTITION BY blockchain, token_address, wallet_address ORDER BY block_time) AS amount_raw
    FROM 
    {{ ref('transfers_gnosis_xdai') }}
)

SELECT * FROM sparse_balances

