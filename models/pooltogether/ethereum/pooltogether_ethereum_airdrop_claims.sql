{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "pooltogether",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'PoolTogether' AS project
, 'PoolTogether Airdrop' AS airdrop_identifier
, account AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, CAST(amount/POWER(10, 18) AS double) AS quantity
, '0x0cec1a9154ff802e7934fc916ed7ca50bde6844e' AS token_address
, 'POOL' AS token_symbol
, evt_index
FROM {{ source('pooltogether_ethereum', 'MerkleDistributor_evt_Claimed') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}