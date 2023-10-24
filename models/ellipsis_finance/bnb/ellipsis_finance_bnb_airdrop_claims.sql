{{
    config(
        tags=['dunesql', 'static'],
        schema = 'ellipsis_finance_bnb',
        alias = alias('airdrop_claims'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "ellipsis_finance",
                                \'["hildobby"]\') }}'
    )
}}

{% set eps_token_address = '0xa7f552078dcc247c2684336020c03648500c6d9f' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'bnb'
    AND contract_address= {{eps_token_address}}
    )

SELECT 'bnb' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'ellipsis_finance' AS project
, 1 AS airdrop_number
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.amount AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, {{eps_token_address}} AS token_address
, 'EPS' AS token_symbol
, t.evt_index
FROM {{ source('ellipsis_finance_bnb', 'AirdropClaim_evt_Claimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'bnb'
    AND pu.contract_address= {{eps_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
WHERE t.evt_block_time BETWEEN timestamp '2021-03-24' AND timestamp '2022-04-01'