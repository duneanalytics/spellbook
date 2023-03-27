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
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Arbitrum' AS project
, 'Arbitrum Airdrop' AS airdrop_identifier
, recipient
, contract_address
, evt_tx_hash AS tx_hash
, CAST(amount/POWER(10, 18) AS double) AS quantity
, '0x912ce59144191c1204e64559fe8253a0e49e6548' AS token_address
, 'ARB' AS token_symbol
, evt_index
FROM {{ source('arbitrum_airdrop_arbitrum', 'TokenDistributor_evt_HasClaimed') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}