{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "tornado_cash",
                                \'["hildobby"]\') }}'
    )
}}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='0x77777feddddffc19ff86db637967013e6c6a116c'
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Tornado Cash' AS project
, 'Tornado Cash Airdrop' AS airdrop_identifier
, t.from AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.value AS DECIMAL(38,0)) AS amount_raw
, CAST(t.value/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.value/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.value/POWER(10, 18) AS double)
    END AS amount_usd
, '0x77777feddddffc19ff86db637967013e6c6a116c' AS token_address
, 'TORN' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0x77777feddddffc19ff86db637967013e6c6a116c'
    AND pu.minute=date_trunc('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
WHERE t.contract_address = '0x3efa30704d2b8bbac821307230376556cf8cc39e'
AND t.to = '0x0000000000000000000000000000000000000000'
AND t.evt_block_time BETWEEN '2020-12-18' AND '2022-01-01'
{% if is_incremental() %}
AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}