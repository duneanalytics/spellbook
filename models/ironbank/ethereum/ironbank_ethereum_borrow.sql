{{ config(
    alias = 'borrow',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
evt_block_number AS block_number,
evt_block_time AS block_time,
evt_tx_hash AS tx_hash,
evt_index AS `index`,
borrower,
itokens.symbol,
itokens.underlying_symbol,
itokens.underlying_token_address AS underlying_address,
borrow.borrowAmount / power(10,itokens.underlying_decimals) AS borrow_amount,
borrow.borrowAmount / power(10,itokens.underlying_decimals)*p.price AS borrow_usd
FROM {{ source('ironbank_ethereum', 'CErc20Delegator_evt_Borrow') }} borrow
LEFT JOIN {{ ref('ironbank_ethereum_itokens') }} itokens ON borrow.contract_address = itokens.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', borrow.evt_block_time) AND p.contract_address = itokens.underlying_token_address
WHERE p.blockchain = 'ethereum'