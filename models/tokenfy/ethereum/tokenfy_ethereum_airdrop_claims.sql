{{
    config(
        alias='airdrop_claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['recipient', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "apecoin",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 'ethereum' AS blockchain
, t.evt_block_time AS block_time
, t.evt_block_number AS block_number
, 'Tokenfy' AS project
, 'Tokenfy Airdrop' AS airdrop_identifier
, t.to AS recipient
, t.contract_address
, t.evt_tx_hash AS tx_hash
, CAST(t.value/POWER(10, 18) AS double) AS quantity
, '0xa6dd98031551c23bb4a2fbe2c4d524e8f737c6f7' AS token_address
, 'TKNFY' AS token_symbol
, t.evt_index
FROM {{ source('erc20_ethereum', 'evt_transfer') }} t
INNER JOIN {{source( 'tokenfy_ethereum', 'Tokenfy_call_claim' ) }} c ON c.call_block_number=t.evt_block_number
    AND c.call_tx_hash=t.evt_tx_hash
    {% if is_incremental() %}
    AND c.call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
WHERE t.contract_address = '0xa6dd98031551c23bb4a2fbe2c4d524e8f737c6f7'
AND t.from = '0x0000000000000000000000000000000000000000'
AND t.evt_block_number BETWEEN 14050661 AND 14141282
{% if is_incremental() %}
AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}