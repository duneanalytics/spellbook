{{
    config(
        alias='airdrop_claims',
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

SELECT 'arbitrum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Arbitrum' AS project
, 'Arbitrum Airdrop' AS airdrop_identifier
, t.recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS DECIMAL(38,0)) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CAST(pu.price*t.amount/POWER(10, 18) AS double) AS amount_usd
, '0x912ce59144191c1204e64559fe8253a0e49e6548' AS token_address
, 'ARB' AS token_symbol
, t.evt_index
FROM {{ source('arbitrum_airdrop_arbitrum', 'TokenDistributor_evt_HasClaimed') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'arbitrum'
    AND pu.contract_address='0x912ce59144191c1204e64559fe8253a0e49e6548'
    AND pu.minute=date_trun('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}