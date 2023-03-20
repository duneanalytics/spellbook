{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "looksrare",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 'ethereum' AS blockchain
, evt_block_time AS block_time
, evt_block_number AS block_number
, 'LooksRare' AS project
, 'LooksRare Airdrop' AS airdrop_identifier
, user AS recipient
, contract_address
, evt_tx_hash AS tx_hash
, amount/POWER(10, 18) AS quantity
, '0xf4d2888d29d722226fafa5d9b24f9164c092421e' AS token_address
, 'LOOKS' AS token_symbol
, evt_index
FROM {{ source('looksrare_ethereum', 'LooksRareAirdrop_evt_AirdropRewardsClaim') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}