{{
    config(
        
        partition_by = ['block_month'],
        schema = 'velodrome_ethereum',
        alias = 'airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "velodrome",
                                \'["hildobby"]\') }}'
    )
}}

{% set velo_token_address = '0x3c8b650257cfb5f272f799f5e2b4e65093a11a05' %}

WITH price_bounds AS (
    SELECT MIN(hour) AS min_hour
    , MAX(hour) AS max_hour
    , MIN_BY(median_price, hour) AS min_price
    , MAX_BY(median_price, hour) AS max_price
    FROM {{ ref('dex_prices') }}
    WHERE blockchain = 'optimism'
    AND contract_address= {{velo_token_address}}
    )

SELECT 'optimism' AS blockchain
, CAST(date_trunc('month', t.evt_block_time) as date) as block_month
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'velodrome' AS project
, 1 AS airdrop_number
, t.to AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.amount AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT min_hour FROM price_bounds) AND t.evt_block_time <= (SELECT max_hour FROM price_bounds) THEN CAST(pu.median_price*t.amount/POWER(10, 18) AS double)
    WHEN t.evt_block_time < (SELECT min_hour FROM price_bounds) THEN CAST((SELECT min_price FROM price_bounds)*t.amount/POWER(10, 18) AS double)
    WHEN t.evt_block_time > (SELECT max_hour FROM price_bounds) THEN CAST((SELECT max_price FROM price_bounds)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, {{velo_token_address}} AS token_address
, 'VELO' AS token_symbol
, t.evt_index
FROM {{ source('velodrome_optimism', 'MerkleClaim_evt_Claim') }} t
LEFT JOIN {{ ref('dex_prices') }} pu ON pu.blockchain = 'optimism'
    AND pu.contract_address= {{velo_token_address}}
    AND pu.hour = date_trunc('hour', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.hour >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' Day)
{% endif %}