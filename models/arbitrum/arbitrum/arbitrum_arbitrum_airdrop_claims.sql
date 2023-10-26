{{
    config(
        
        schema = 'arbitrum_arbitrum',
        alias = 'airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "arbitrum",
                                \'["hildobby"]\') }}'
    )
}}

{% set arb_token_address = '0x912ce59144191c1204e64559fe8253a0e49e6548' %}

WITH early_price AS (
    SELECT MIN(minute) AS minute
    , MIN_BY(price, minute) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'arbitrum'
    AND contract_address= {{arb_token_address}}
    )

SELECT 'arbitrum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'arbitrum' AS project
, 1 AS airdrop_number
, t.recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, t.amount as amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CASE WHEN t.evt_block_time >= (SELECT minute FROM early_price) THEN CAST(pu.price*t.amount/POWER(10, 18) AS double)
    ELSE CAST((SELECT price FROM early_price)*t.amount/POWER(10, 18) AS double)
    END AS amount_usd
, {{arb_token_address}} AS token_address
, 'ARB' AS token_symbol
, t.evt_index
FROM {{ source('arbitrum_arbitrum', 'TokenDistributor_evt_HasClaimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'arbitrum'
    AND pu.contract_address= {{arb_token_address}}
    AND pu.minute=date_trunc('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' Day)
{% endif %}