{{
    config(
        schema = 'etherfi_ethereum',
        alias = 'airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        unique_key = ['tx_hash', 'evt_index', 'recipient'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "etherfi",
                                \'["hildobby"]\') }}'
    )
}}

{% set etherfi_token_address = '0xfe0c30065b384f05761f15d0cc899d4f9f9cc0eb' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address= {{etherfi_token_address}}
    )

SELECT DISTINCT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'etherfi' AS project
, 1 AS airdrop_number
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.amount AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, {{etherfi_token_address}} AS token_address
, 'ETHFI' AS token_symbol
, t.evt_index
FROM {{ source('ethfi_ethereum', 'MerkleDistributorWithDeadline_evt_Claimed') }} t
LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address= {{etherfi_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
{% if is_incremental() %}
WHERE {{ incremental_predicate('t.evt_block_time') }}
{% else %}
WHERE t.evt_block_time >= CAST('2024-03-18' as TIMESTAMP)
{% endif %}