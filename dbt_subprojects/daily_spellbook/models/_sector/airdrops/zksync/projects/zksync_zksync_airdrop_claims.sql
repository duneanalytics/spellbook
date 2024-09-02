{{
    config(

        schema = 'zksync_zksync',
        alias = 'airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index']
    )
}}

{% set token_address = '0x5A7d6b2F92C77FAD6CCaBd7EE0624E64907Eaf3E' %} -- ZK token

WITH early_price AS (
    SELECT 
        MIN(minute) AS minute
        , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'zksync'
    AND contract_address = {{token_address}}
)

SELECT 
    'zksync' AS blockchain
    , t.evt_block_time AS block_time
    , t.evt_block_number AS block_number
    , 'zksync' AS project
    , 1 AS airdrop_number
    , t.account AS recipient
    , t.contract_address
    , t.evt_tx_hash AS tx_hash
    , t.amount as amount_raw
    , ROUND(CAST(t.amount/POWER(10, 18) AS double), 0) AS amount_original
    , CASE 
        WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double) 
        ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
      END AS amount_usd
    , {{token_address}} AS token_address
    , 'ZK' AS token_symbol
    , t.evt_index
FROM {{ source('zksync_era_zksync', 'ZkMerkleDistributor_evt_Claimed') }} t
LEFT JOIN {{ source('prices','usd_forward_fill') }} pu ON pu.blockchain = 'zksync'
    AND pu.contract_address = {{token_address}}
    AND pu.minute = date_trunc('minute', t.evt_block_time)
{% if is_incremental() %}
    AND {{incremental_predicate('pu.minute')}}
WHERE {{incremental_predicate('t.evt_block_time')}}
{% endif %}
