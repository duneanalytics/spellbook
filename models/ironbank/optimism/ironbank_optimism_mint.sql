{{ config(
    alias = 'mint',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
evt_block_number AS block_number,
evt_block_time AS block_time,
evt_tx_hash AS tx_hash,
evt_index AS `index`,
minter,
itokens.symbol,
itokens.underlying_symbol,
itokens.underlying_token_address AS underlying_address,
mintAmount / power(10,itokens.underlying_decimals) AS mint_amount,
mintAmount / power(10,itokens.underlying_decimals)*p.price AS mint_usd
FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_Mint') }} mint
LEFT JOIN {{ ref('ironbank_optimism_itokens') }} itokens ON mint.contract_address = itokens.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', mint.evt_block_time) AND p.contract_address = itokens.underlying_token_address
WHERE p.blockchain = 'optimism'