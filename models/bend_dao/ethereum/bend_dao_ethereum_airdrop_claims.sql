{{
    config(
        tags=[ 'static'],
        schema = 'bend_dao_ethereum',
        alias = 'airdrop_claims',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "bend_dao",
                                \'["hildobby"]\') }}'
    )
}}

{% set bend_token_address = '0x0d02755a5700414b26ff040e1de35d337df56218' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address= {{bend_token_address}}
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'benddao' AS project
, 1 AS airdrop_number
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.amount AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, {{bend_token_address}} AS token_address
, 'BEND' AS token_symbol
, t.evt_index
FROM {{ source('benddao_ethereum', 'MerkleDistributor_evt_Claimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address= {{bend_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN CAST('2022-03-21' as timestamp) AND CAST('2022-06-19' as timestamp)