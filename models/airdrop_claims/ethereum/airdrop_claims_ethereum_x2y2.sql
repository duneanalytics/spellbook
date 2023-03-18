{{
    config(
        alias='x2y2',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "airdrop",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'X2Y2' AS project
, 'X2Y2 Airdrop' AS airdrop_identifier
, to AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, value/POWER(10, 18) AS quantity
, '0x1e4ede388cbc9f4b5c79681b7f94d36a11abebc9' AS token_address
, 'X2Y2' AS token_symbol
, evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }}
WHERE contract_address = '0x1e4ede388cbc9f4b5c79681b7f94d36a11abebc9'
AND from = '0xe6949137b24ad50cce2cf6b124b3b874449a41fa'
AND to <> '0xc8c3cc5be962b6d281e4a53dbcce1359f76a1b85'
AND evt_block_time > '2022-02-15'
{% if is_incremental() %}
AND evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}