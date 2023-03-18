{{
    config(
        alias='tornado_cash',
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
, 'Tornado Cash' AS project
, 'Tornado Cash Airdrop' AS airdrop_identifier
, from AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, value/POWER(10, 18) AS quantity
, '0x77777feddddffc19ff86db637967013e6c6a116c' AS token_address
, 'TORN' AS token_symbol
FROM {{ source('erc20_ethereum', 'evt_transfer') }}
WHERE contract_address = '0x3efa30704d2b8bbac821307230376556cf8cc39e'
AND to = '0x0000000000000000000000000000000000000000'
AND block_time > '2020-12-18'
{% if is_incremental() %}
AND evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}