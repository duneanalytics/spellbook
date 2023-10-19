{{
    config(
        schema = 'apecoin_ethereum',
        alias = alias('airdrop_claims', legacy_model=True),
        materialized = 'table',
        file_format = 'delta',
        tags=['legacy', 'static'],
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "apecoin",
                                \'["hildobby"]\') }}'
    )
}}

{% set ape_token_address = '0x4d224452801aced8b2f0aebe155379bb5d594381' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='{{ape_token_address}}'
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'ApeCoin' AS project
, 'ApeCoin Airdrop' AS airdrop_identifier
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS DECIMAL(38,0)) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, '{{ape_token_address}}' AS token_address
, 'APE' AS token_symbol
, t.evt_index
FROM {{ source('apecoin_ethereum', 'AirdropGrapesToken_evt_AirDrop') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill_legacy') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='{{ape_token_address}}'
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN '2022-03-17' AND '2022-06-16'