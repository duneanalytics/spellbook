{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "botto",
                                \'["hildobby"]\') }}'
    )
}}


SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Botto' AS project
, 'Botto Airdrop' AS airdrop_identifier
, to AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, CAST(amount/POWER(10, 18) AS double) AS quantity
, '0x9dfad1b7102d46b1b197b90095b5c4e9f5845bba' AS token_address
, 'BOTTO' AS token_symbol
, evt_index
FROM {{ source('botto_ethereum', 'BottoAirdrop_evt_AirdropTransfer') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}