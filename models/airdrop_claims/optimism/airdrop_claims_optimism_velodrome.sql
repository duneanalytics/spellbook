{{
    config(
        alias='velodrome',
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

SELECT 'optimism' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Velodrome' AS project
, 'Velodrome Airdrop' AS airdrop_identifier
, to AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, amount/POWER(10, 18) AS quantity
, '0x3c8b650257cfb5f272f799f5e2b4e65093a11a05' AS token_address
, 'VELO' AS token_symbol
, evt_index
FROM {{ source('velodrome_optimism', 'MerkleClaim_evt_Claim') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}