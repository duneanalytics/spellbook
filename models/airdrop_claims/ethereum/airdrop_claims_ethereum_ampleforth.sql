{{
    config(
        alias='ampleforth',
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
, 'Ampleforth' AS project
, 'Ampleforth Forth Airdrop' AS airdrop_identifier
, to AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, value/POWER(10, 18) AS quantity
, '0x77fba179c79de5b7653f68b5039af940ada60ce0' AS token_address
, 'FORTH' AS token_symbol
FROM {{ source('erc20_ethereum', 'evt_transfer') }}
WHERE contract_address = '0x77fba179c79de5b7653f68b5039af940ada60ce0'
AND from = '0xf497b83cfbd31e7ba1ab646f3b50ae0af52d03a1'
AND evt_block_time > '2021-04-20'
{% if is_incremental() %}
AND evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}