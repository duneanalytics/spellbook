{{
    config(
        schema = 'cow_protocol_ethereum',
        alias = alias('airdrop_claims'),
        materialized = 'table',
        file_format = 'delta',
        tags=['static', 'dunesql'],
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "cow_protocol",
                                \'["hildobby", "bh2smith"]\') }}'
    )
}}

{% set cow_token_address = '0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address={{cow_token_address}}
    )

SELECT
    'ethereum' AS blockchain,
    t.evt_block_time AS block_time,
    t.evt_block_number AS block_number,
    'cow_protocol' AS project,
    1 AS airdrop_number,
    t.claimant AS recipient,
    t.contract_address,
    t.evt_tx_hash AS tx_hash,
    t.claimedAmount AS amount_raw,
    t.claimedAmount/POW(10, 18) AS amount_original,
    CASE
        WHEN t.evt_block_time >= (SELECT minute FROM early_price)
        THEN pu.price*t.claimedAmount / POW(10, 18)
        ELSE (SELECT price FROM early_price) * t.claimedAmount /POW(10, 18)
    END AS amount_usd,
    {{cow_token_address}} AS token_address,
    'COW' AS token_symbol,
    t.evt_index
FROM {{ source('cow_protocol_ethereum', 'CowProtocolVirtualToken_evt_Claimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address={{cow_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN timestamp '2022-02-11' AND timestamp '2022-03-26'