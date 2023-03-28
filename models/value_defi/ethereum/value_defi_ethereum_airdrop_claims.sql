{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "value_defi",
                                \'["hildobby"]\') }}'
    )
}}

WITH early_price AS (
    SELECT MIN(hour) AS hour
    , MIN_BY(median_price, hour) AS price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='0x3479b0acf875405d7853f44142fe06470a40f6cc'
    )

, late_price AS (
    SELECT MAX(hour) AS hour
    , MAX_BY(median_price, hour) AS price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='0x3479b0acf875405d7853f44142fe06470a40f6cc'
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Value DeFi' AS project
, 'Value DeFi Airdrop' AS airdrop_identifier
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS DECIMAL(38,0)) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT hour FROM early_price) AND t.evt_block_time <= (SELECT hour FROM late_price) THEN CAST(pu.median_price*t.amount/POWER(10, 18) AS double)
    WHEN t.evt_block_time < (SELECT hour FROM early_price) THEN CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    WHEN t.evt_block_time > (SELECT hour FROM late_price) THEN CAST((SELECT price FROM late_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, '0x3479b0acf875405d7853f44142fe06470a40f6cc' AS token_address
, 'vUSD' AS token_symbol
, t.evt_index
FROM {{ source('value_defi_ethereum', 'MerkleDistributor_evt_Claimed') }} t
LEFT JOIN {{ ref('dex_prices') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0x3479b0acf875405d7853f44142fe06470a40f6cc'
    AND pu.hour = date_trunc('hour', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.hour >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}