{{
    config(
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


SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Hop Protocol' AS project
, 'Hop Protocol Airdrop' AS airdrop_identifier
, claimant AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, amount/POWER(10, 18) AS quantity
, '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc' AS token_address
, 'HOP' AS token_symbol
, evt_index
FROM {{ source('hop_protocol_ethereum', 'HOPToken_evt_Claim') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}