{{
    config(
        alias='dydx',
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
, 'DYDX' AS project
, 'DYDX Airdrop' AS airdrop_identifier
, to AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, value/POWER(10, 18) AS quantity
, '0x92d6c1e31e14520e676a687f0a93788b716beff5' AS token_address
, 'DYDX' AS token_symbol
, evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }}
WHERE contract_address = '0x92d6c1e31e14520e676a687f0a93788b716beff5'
AND from = '0x639192d54431f8c816368d3fb4107bc168d0e871'
AND evt_block_time > '2021-09-08'
{% if is_incremental() %}
AND evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}