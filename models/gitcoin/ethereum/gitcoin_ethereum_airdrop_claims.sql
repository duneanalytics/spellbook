{{
    config(
        tags=[ 'static'],
        schema = 'gitcoin_ethereum',
        alias = 'airdrop_claims',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "gitcoin",
                                \'["hildobby"]\') }}'
    )
}}

{% set gtc_token_address = '0xde30da39c46104798bb5aa3fe8b9e0e1f348163f' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address= {{gtc_token_address}}
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'gitcoin' AS project
, 1 AS airdrop_number
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.amount AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, {{gtc_token_address}} AS token_address
, 'GTC' AS token_symbol
, t.evt_index
FROM {{ source('gitcoin_ethereum', 'TokenDistributor_evt_Claimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address= {{gtc_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN timestamp '2021-05-24' AND timestamp '2021-06-24'