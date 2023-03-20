{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "forta_network",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Forta Network' AS project
, 'Forta Network Airdrop' AS airdrop_identifier
, recipient
, contract_address
, evt_tx_hash AS tx_hash
, amount/POWER(10, 18) AS quantity
, '0x41545f8b9472d758bb669ed8eaeeecd7a9c4ec29' AS token_address
, 'FORT' AS token_symbol
, evt_index
FROM {{ source('forta_network_ethereum', 'Airdrop_evt_TokensReleased') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}