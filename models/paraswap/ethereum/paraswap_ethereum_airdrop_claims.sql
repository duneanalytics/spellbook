{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "paraswap",
                                \'["hildobby"]\') }}'
    )
}}

WITH early_price AS (
    SELECT MIN(hour) AS hour
    , MIN_BY(median_price, hour) AS price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='0xcafe001067cdef266afb7eb5a286dcfd277f3de5'
    )

, late_price AS (
    SELECT MAX(hour) AS hour
    , MAX_BY(median_price, hour) AS price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='0xcafe001067cdef266afb7eb5a286dcfd277f3de5'
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Paraswap' AS project
, 'Paraswap Airdrop' AS airdrop_identifier
, from AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.value AS DECIMAL(38,0)) AS amount_raw
, CAST(t.value/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT hour FROM early_price) AND t.evt_block_time <= (SELECT hour FROM late_price) THEN CAST(pu.median_price*t.value/POWER(10, 18) AS double)
    WHEN t.evt_block_time < (SELECT hour FROM early_price) THEN CAST((SELECT price FROM early_price)*t.value/POWER(10, 18) AS double)
    WHEN t.evt_block_time > (SELECT hour FROM late_price) THEN CAST((SELECT price FROM late_price)*t.value/POWER(10, 18) AS double)
    END AS amount_usd
, '0xcafe001067cdef266afb7eb5a286dcfd277f3de5' AS token_address
, 'PSP' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('dex_prices') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0xcafe001067cdef266afb7eb5a286dcfd277f3de5'
    AND pu.hour = date_trunc('hour', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.hour >= date_trunc("day", now() - interval '1 week')
    {% endif %}
WHERE t.contract_address = '0xcafe001067cdef266afb7eb5a286dcfd277f3de5'
AND t.from = '0x090e53c44e8a9b6b1bca800e881455b921aec420'
AND t.evt_block_time > '2021-11-15'
{% if is_incremental() %}
AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}