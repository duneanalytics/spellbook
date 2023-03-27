{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "notional",
                                \'["hildobby"]\') }}'
    )
}}


SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Notional Finance' AS project
, 'Notional Finance Airdrop' AS airdrop_identifier
, account AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, CAST(amount/POWER(10, 18) AS double) AS quantity
, '0xcfeaead4947f0705a14ec42ac3d44129e1ef3ed5' AS token_address
, 'NOTE' AS token_symbol
, evt_index
FROM {{ source('notional_ethereum', 'MerkleDistributor_evt_Claimed') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}