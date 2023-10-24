{{
    config(
        
        partition_by = ['block_month'],
        schema = 'safe_ethereum',
        alias = 'airdrop_claims',
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

WITH more_prices AS (
    SELECT MIN(hour) AS min_hour
    , MAX(hour) AS max_hour
    , MIN_BY(median_price, hour) AS min_price
    , MAX_BY(median_price, hour) AS max_price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'ethereum'
    AND contract_address= {{safe_token_address}}
    )

SELECT 'ethereum' AS blockchain
, CAST(date_trunc('month', t.evt_block_time) as date) as block_month
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'safe' AS project
, 1 AS airdrop_number
, t.to AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.value AS amount_raw
, CAST(t.value/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT min_hour FROM more_prices) AND t.evt_block_time <= (SELECT max_hour FROM more_prices) THEN CAST(pu.median_price*t.value/POWER(10, 18) AS double)
    WHEN t.evt_block_time < (SELECT min_hour FROM more_prices) THEN CAST((SELECT min_price FROM more_prices)*t.value/POWER(10, 18) AS double)
    WHEN t.evt_block_time > (SELECT max_hour FROM more_prices) THEN CAST((SELECT max_price FROM more_prices)*t.value/POWER(10, 18) AS double)
    END AS amount_usd
, {{safe_token_address}} AS token_address
, 'SAFE' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('dex_prices') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address= {{safe_token_address}}
    AND pu.hour = date_trunc('hour', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.hour >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
WHERE t.contract_address = {{safe_token_address}}
AND t."from" = 0xa0b937d5c8e32a80e3a8ed4227cd020221544ee6
AND t.evt_block_time >= TIMESTAMP '2022-09-28'
{% if is_incremental() %}
AND t.evt_block_time >= date_trunc('day', now() - interval '7' Day)
{% endif %}