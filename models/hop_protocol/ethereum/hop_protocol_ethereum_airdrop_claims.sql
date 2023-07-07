{{
    config(
        schema = 'hop_protocol_ethereum',
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "hop_protocol",
                                \'["hildobby"]\') }}'
    )
}}

{% set hop_token_address = '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'ethereum'
    AND contract_address='{{hop_token_address}}'
    )

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Hop Protocol' AS project
, 'Hop Protocol Airdrop' AS airdrop_identifier
, t.claimant AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS DECIMAL(38,0)) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, '{{hop_token_address}}' AS token_address
, 'HOP' AS token_symbol
, t.evt_index
FROM {{ source('hop_protocol_ethereum', 'HOPToken_evt_Claim') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill_legacy') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='{{hop_token_address}}'
    AND pu.minute=date_trunc('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}