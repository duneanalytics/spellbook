{{
    config(
        alias='ens',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "airdrop",
                                \'["hildobby"]\') }}'
    )
}}


SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Ethereum Name Service' AS project
, 'Ethereum Name Service Airdrop' AS airdrop_identifier
, claimant AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, amount/POWER(10, 18) AS quantity
, '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72' AS token_address
, 'ENS' AS token_symbol
FROM {{ source('ethereumnameservice_ethereum', 'ENSToken_evt_Claim') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}