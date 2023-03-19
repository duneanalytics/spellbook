{{
    config(
        alias='safe',
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

SELECT 'optimism' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Safe' AS project
, 'Safe Airdrop' AS airdrop_identifier
, to AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, value/POWER(10, 18) AS quantity
, '0x5afe3855358e112b5647b952709e6165e1c1eeee' AS token_address
, 'SAFE' AS token_symbol
, evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }}
WHERE contract_address = '0x5afe3855358e112b5647b952709e6165e1c1eeee'
AND from = '0xa0b937d5c8e32a80e3a8ed4227cd020221544ee6'
AND evt_block_time > '2022-09-28'
{% if is_incremental() %}
AND evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}