{{
    config(
        tags=['dunesql', 'static'],
        schema = 'x2y2_ethereum',
        alias = alias('airdrop_claims'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "x2y2",
                                \'["hildobby"]\') }}'
    )
}}

{% set xtyt_token_address = '0x1e4ede388cbc9f4b5c79681b7f94d36a11abebc9' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address= {{xtyt_token_address}}
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'x2y2' AS project
, 1 AS airdrop_number
, t.to AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.value AS amount_raw
, CAST(t.value/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.value/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.value/POWER(10, 18) AS double)
    END AS amount_usd
, {{xtyt_token_address}} AS token_address
, 'X2Y2' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address= {{xtyt_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN timestamp '2022-02-15' AND timestamp '2022-03-31'
    AND t.contract_address = {{xtyt_token_address}}
