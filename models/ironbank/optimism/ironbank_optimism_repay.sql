{{ config(
    alias = 'repay',
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
borrower,
itokens.symbol,
itokens.underlying_symbol,
itokens.underlying_token_address AS underlying_address,
repayAmount / power(10,itokens.underlying_decimals) AS repay_amount,
repayAmount / power(10,itokens.underlying_decimals)*p.price AS repay_usd
FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_RepayBorrow') }} repay
LEFT JOIN {{ ref('ironbank_optimism_itokens') }} itokens ON itokens.contract_address = repay.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', repay.evt_block_time) AND p.contract_address = itokens.underlying_token_address
WHERE p.blockchain = 'optimism'