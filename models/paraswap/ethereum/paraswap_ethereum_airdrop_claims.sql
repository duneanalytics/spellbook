{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "paraswap",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Paraswap' AS project
, 'Paraswap Airdrop' AS airdrop_identifier
, from AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.value AS DECIMAL(38,0)) AS amount_raw
, CAST(t.value/POWER(10, 18) AS double) AS amount_original
, CAST(pu.price*t.value/POWER(10, 18) AS double) AS amount_usd
, '0xcafe001067cdef266afb7eb5a286dcfd277f3de5' AS token_address
, 'PSP' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0xcafe001067cdef266afb7eb5a286dcfd277f3de5'
    AND pu.minute=date_trun('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
WHERE t.contract_address = '0xcafe001067cdef266afb7eb5a286dcfd277f3de5'
AND t.from = '0x090e53c44e8a9b6b1bca800e881455b921aec420'
AND t.evt_block_time > '2021-11-15'
{% if is_incremental() %}
AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}