{{
    config(
        tags=['dunesql', 'static'],
        schema = 'ampleforth_ethereum',
        alias = alias('airdrop_claims'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ampleforth",
                                \'["hildobby"]\') }}'
    )
}}

{% set forth_token_address = '0x77fba179c79de5b7653f68b5039af940ada60ce0' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address= {{forth_token_address}}
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Ampleforth' AS project
, 'Ampleforth Forth Airdrop' AS airdrop_identifier
, t.to AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.value AS amount_raw
, CAST(t.value/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.value/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.value/POWER(10, 18) AS double)
    END AS amount_usd
, {{forth_token_address}} AS token_address
, 'FORTH' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address= {{forth_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN CAST('2021-04-20' as timestamp) AND CAST('2022-04-16' as timestamp)
    AND t.contract_address = {{forth_token_address}}
