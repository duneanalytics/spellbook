{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "value_defi",
                                \'["hildobby"]\') }}'
    )
}}


SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'Value DeFi' AS project
, 'Value DeFi Airdrop' AS airdrop_identifier
, account AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, CAST(amount/POWER(10, 18) AS double) AS quantity
, '0x3479b0acf875405d7853f44142fe06470a40f6cc' AS token_address
, 'vUSD' AS token_symbol
, evt_index
FROM {{ source('value_defi_ethereum', 'MerkleDistributor_evt_Claimed') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}