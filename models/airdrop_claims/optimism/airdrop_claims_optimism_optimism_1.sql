{{
    config(
        alias='optimism_1',
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
, 'Optimism' AS project
, 'Optimism Airdrop 1' AS airdrop_identifier
, account AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, amount/POWER(10, 18) AS quantity
, '0x4200000000000000000000000000000000000042' AS token_address
, 'OP' AS token_symbol
, evt_index
FROM {{ source('op_optimism', 'MerkleDistributor_evt_Claimed') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}