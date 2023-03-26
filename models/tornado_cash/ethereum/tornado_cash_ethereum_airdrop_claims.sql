{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "tornado_cash",
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
, CAST(value/POWER(10, 18) AS double) AS quantity
, '0x77777feddddffc19ff86db637967013e6c6a116c' AS token_address
, 'TORN' AS token_symbol
, evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }}
WHERE contract_address = '0x3efa30704d2b8bbac821307230376556cf8cc39e'
AND to = '0x0000000000000000000000000000000000000000'
AND evt_block_time BETWEEN '2020-12-18' AND '2022-01-01'
{% if is_incremental() %}
AND evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}