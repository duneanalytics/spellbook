{{
    config(
        alias='blur_1',
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
, 'Blur' AS project
, 'Blur Airdrop 1' AS airdrop_identifier
, account AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, amount/POWER(10, 18) AS quantity
, '0x5283d291dbcf85356a21ba090e6db59121208b44' AS token_address
, 'BLUR' AS token_symbol
, evt_index
FROM {{ source('blur_ethereum', 'BlurAirdrop_evt_Claimed') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}