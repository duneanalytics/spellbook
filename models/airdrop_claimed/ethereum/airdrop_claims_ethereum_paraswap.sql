{{
    config(
        alias='paraswap',
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
, 'Paraswap' AS project
, 'Paraswap Airdrop' AS airdrop_identifier
, from AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, value/POWER(10, 18) AS quantity
, '0xcafe001067cdef266afb7eb5a286dcfd277f3de5' AS token_address
, 'PSP' AS token_symbol
FROM {{ source('erc20_ethereum', 'evt_transfer') }}
WHERE contract_address = '0xcafe001067cdef266afb7eb5a286dcfd277f3de5'
AND from = '0x090e53c44e8a9b6b1bca800e881455b921aec420'
AND block_time > '2021-11-15'
{% if is_incremental() %}
AND evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}