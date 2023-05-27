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

{% set psp_token_address = '0xcafe001067cdef266afb7eb5a286dcfd277f3de5' %}

WITH more_prices AS (
    SELECT MIN(hour) AS min_hour
    , MAX(hour) AS max_hour
    , MIN_BY(median_price, hour) AS min_price
    , MAX_BY(median_price, hour) AS max_price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='{{psp_token_address}}'
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
, CASE WHEN t.evt_block_time >= (SELECT min_hour FROM more_prices) AND t.evt_block_time <= (SELECT max_hour FROM more_prices) THEN CAST(pu.median_price*t.value/POWER(10, 18) AS double)
    WHEN t.evt_block_time < (SELECT min_hour FROM more_prices) THEN CAST((SELECT min_price FROM more_prices)*t.value/POWER(10, 18) AS double)
    WHEN t.evt_block_time > (SELECT max_hour FROM more_prices) THEN CAST((SELECT max_price FROM more_prices)*t.value/POWER(10, 18) AS double)
    END AS amount_usd
, '{{psp_token_address}}' AS token_address
, 'PSP' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('dex_prices') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='{{psp_token_address}}'
    AND pu.hour = date_trunc('hour', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.hour >= date_trunc("day", now() - interval '1 week')
    {% endif %}
WHERE t.contract_address = '{{psp_token_address}}'
AND t.from = '0x090e53c44e8a9b6b1bca800e881455b921aec420'
AND t.evt_block_time > '2021-11-15'
{% if is_incremental() %}
AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}