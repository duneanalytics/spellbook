{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ens",
                                \'["hildobby"]\') }}'
    )
}}


SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Ethereum Name Service' AS project
, 'Ethereum Name Service Airdrop' AS airdrop_identifier
, t.claimant AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS DECIMAL(38,0)) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CAST(pu.price*t.amount/POWER(10, 18) AS double) AS amount_usd
, '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72' AS token_address
, 'ENS' AS token_symbol
, t.evt_index
FROM {{ source('ethereumnameservice_ethereum', 'ENSToken_evt_Claim') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0xc18360217d8f7ab5e7c516566761ea12ce7f9d72'
    AND pu.minute=date_trun('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}