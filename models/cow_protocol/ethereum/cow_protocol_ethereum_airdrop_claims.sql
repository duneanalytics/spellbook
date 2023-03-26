{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "cow_protocol",
                                \'["hildobby"]\') }}'
    )
}}


SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'CoW Protocol' AS project
, 'CoW Protocol Airdrop' AS airdrop_identifier
, claimant AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, CAST(claimedAmount/POWER(10, 18) AS double) AS quantity
, '0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab' AS token_address
, 'COW' AS token_symbol
, evt_index
FROM {{ source('cow_protocol_ethereum', 'CowProtocolVirtualToken_evt_Claimed') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}