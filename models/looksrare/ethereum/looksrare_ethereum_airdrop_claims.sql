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
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'LooksRare' AS project
, 'LooksRare Airdrop' AS airdrop_identifier
, t.user AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.amount AS DECIMAL(38,0)) AS amount_raw
, CAST(t.amount/POWER(10, 18) AS double) AS amount_original
, CAST(pu.price*t.amount/POWER(10, 18) AS double) AS amount_usd
, '0xf4d2888d29d722226fafa5d9b24f9164c092421e' AS token_address
, 'LOOKS' AS token_symbol
, t.evt_index
FROM {{ source('looksrare_ethereum', 'LooksRareAirdrop_evt_AirdropRewardsClaim') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0xf4d2888d29d722226fafa5d9b24f9164c092421e'
    AND pu.minute=date_trun('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}