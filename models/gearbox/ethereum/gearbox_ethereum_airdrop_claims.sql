{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        tags=['static'],
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "gearbox",
                                \'["hildobby"]\') }}'
    )
}}

{% set gear_token_address = '0xba3335588d9403515223f109edc4eb7269a9ab5d' %}

WITH early_price AS (
    SELECT MIN(hour) AS hour
    , MIN_BY(median_price, hour) AS price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='{{gear_token_address}}'
    )

, late_price AS (
    SELECT MAX(hour) AS hour
    , MAX_BY(median_price, hour) AS price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='{{gear_token_address}}'
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Gearbox Protocol' AS project
, 'Gearbox Protocol Airdrop' AS airdrop_identifier
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS DECIMAL(38,0)) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT hour FROM early_price) AND t.evt_block_time <= (SELECT hour FROM late_price) THEN CAST(pu.median_price*t.amount/POWER(10, 18) AS double)
    WHEN t.evt_block_time < (SELECT hour FROM early_price) THEN CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    WHEN t.evt_block_time > (SELECT hour FROM late_price) THEN CAST((SELECT price FROM late_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, '{{gear_token_address}}' AS token_address
, 'GEAR' AS token_symbol
, t.evt_index
FROM {{ source('gearbox_ethereum', 'MerkleDistributor_evt_Claimed') }} t
LEFT JOIN {{ ref('dex_prices') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='{{gear_token_address}}'
    AND pu.hour = date_trunc('hour', t.evt_block_time)
WHERE t.evt_block_time BETWEEN '	
2022-04-05' AND '2022-07-22'