{{
    config(
        tags=[ 'static'],
        schema = 'forta_network_ethereum',
        alias = 'airdrop_claims',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "forta_network",
                                \'["hildobby"]\') }}'
    )
}}

{% set forta_token_address = '0x41545f8b9472d758bb669ed8eaeeecd7a9c4ec29' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address= {{forta_token_address}}
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'forta_network' AS project
, 1 AS airdrop_number
, t.recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.amount AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, {{forta_token_address}} AS token_address
, 'FORT' AS token_symbol
, t.evt_index
FROM {{ source('forta_network_ethereum', 'Airdrop_evt_TokensReleased') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address= {{forta_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN timestamp '2022-06-14' AND timestamp '2022-09-16'