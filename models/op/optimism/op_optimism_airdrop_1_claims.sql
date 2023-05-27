{{
    config(
        schema = 'op_optimism',
        alias='airdrop_1_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["optimsm"]\',
                                "project",
                                "airdrop",
                                \'["hildobby"]\') }}'
    )
}}

{% set op_token_address = '0x4200000000000000000000000000000000000042' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'optimimsm'
    AND contract_address='{{op_token_address}}'
    )

SELECT 'optimism' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Optimism' AS project
, 'Optimism Airdrop 1' AS airdrop_identifier
, t.account AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS DECIMAL(38,0)) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, '{{op_token_address}}' AS token_address
, 'OP' AS token_symbol
, t.evt_index
FROM {{ source('op_optimism', 'MerkleDistributor_evt_Claimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'optimism'
    AND pu.contract_address='{{op_token_address}}'
    AND pu.minute=date_trunc('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}