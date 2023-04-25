{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "safe",
                                \'["hildobby"]\') }}'
    )
}}

{% set safe_token_address = '0x5afe3855358e112b5647b952709e6165e1c1eeee' %}

WITH early_price AS (
    SELECT MIN(hour) AS hour
    , MIN_BY(median_price, hour) AS price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='{{safe_token_address}}'
    )

, late_price AS (
    SELECT MAX(hour) AS hour
    , MAX_BY(median_price, hour) AS price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='{{safe_token_address}}'
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Safe' AS project
, 'Safe Airdrop' AS airdrop_identifier
, t.to AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.value AS DECIMAL(38,0)) AS amount_raw
, CAST(t.value/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT hour FROM early_price) AND t.evt_block_time <= (SELECT hour FROM late_price) THEN CAST(pu.median_price*t.value/POWER(10, 18) AS double)
    WHEN t.evt_block_time < (SELECT hour FROM early_price) THEN CAST((SELECT price FROM early_price)*t.value/POWER(10, 18) AS double)
    WHEN t.evt_block_time > (SELECT hour FROM late_price) THEN CAST((SELECT price FROM late_price)*t.value/POWER(10, 18) AS double)
    END AS amount_usd
, '{{safe_token_address}}' AS token_address
, 'SAFE' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('dex_prices') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='{{safe_token_address}}'
    AND pu.hour = date_trunc('hour', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.hour >= date_trunc("day", now() - interval '1 week')
    {% endif %}
WHERE t.contract_address = '{{safe_token_address}}'
AND t.from = '0xa0b937d5c8e32a80e3a8ed4227cd020221544ee6'
AND t.evt_block_time > '2022-09-28'
{% if is_incremental() %}
AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}