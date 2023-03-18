{{
    config(
        alias='dappradar',
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
, 'Dapp Radar' AS project
, 'Dapp Radar Airdrop' AS airdrop_identifier
, recipient
, contract_address
, evt_tx_hash AS tx_hash
, amount/POWER(10, 18) AS quantity
, '0x44709a920fccf795fbc57baa433cc3dd53c44dbe' AS token_address
, 'RADAR' AS token_symbol
FROM {{ source('dappradar_ethereum', 'Airdrop_evt_TokenClaimed') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}