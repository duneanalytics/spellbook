{{ config(
    alias = alias('mint'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
m.evt_block_number AS block_number,
m.evt_block_time AS block_time,
m.evt_tx_hash AS tx_hash,
m.evt_index AS `index`,
CAST(m.contract_address AS VARCHAR(100)) AS contract_address,
m.minter,
i.symbol,
i.underlying_symbol,
i.underlying_token_address AS underlying_address,
CAST(m.mintAmount AS DOUBLE) / power(10,i.underlying_decimals) AS mint_amount,
CAST(m.mintAmount AS DOUBLE) / power(10,i.underlying_decimals)*p.price AS mint_usd
FROM {{ source('ironbank_ethereum', 'CErc20Delegator_evt_Mint') }} m
LEFT JOIN {{ ref('ironbank_ethereum_itokens') }} i ON CAST(m.contract_address AS VARCHAR(100)) = i.contract_address
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', m.evt_block_time) AND CAST(p.contract_address AS VARCHAR(100)) = i.underlying_token_address AND p.blockchain = 'ethereum'