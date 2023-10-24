{{
    config(
        tags=['dunesql', 'static'],
        schema = 'ens_ethereum',
        alias = alias('airdrop_claims'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ens",
                                \'["hildobby"]\') }}'
    )
}}

{% set ens_token_address = '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address= {{ens_token_address}}
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'ethereum_name_service' AS project
, 1 AS airdrop_number
, t.claimant AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.amount AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, {{ens_token_address}} AS token_address
, 'ENS' AS token_symbol
, t.evt_index
FROM {{ source('ethereumnameservice_ethereum', 'ENSToken_evt_Claim') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address= {{ens_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN timestamp '2021-11-09' AND timestamp '2022-11-25'
