{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "dydx",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'DYDX' AS project
, 'DYDX Airdrop' AS airdrop_identifier
, t.to AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.value AS DECIMAL(38,0)) AS amount_raw
, CAST(t.value/POWER(10, 18) AS double) AS amount_original
, CAST(pu.price*t.value/POWER(10, 18) AS double) AS amount_usd
, '0x92d6c1e31e14520e676a687f0a93788b716beff5' AS token_address
, 'DYDX' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address='0x92d6c1e31e14520e676a687f0a93788b716beff5'
    AND pu.minute=date_trunc('minute', t.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
WHERE t.contract_address = '0x92d6c1e31e14520e676a687f0a93788b716beff5'
AND t.from = '0x639192d54431f8c816368d3fb4107bc168d0e871'
AND t.evt_block_time > '2021-09-08'
{% if is_incremental() %}
AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}