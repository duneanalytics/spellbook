{{ config(
    alias = 'redeem',
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
redeemer,
itokens.symbol,
itokens.underlying_symbol,
itokens.underlying_token_address AS underlying_address,
redeemAmount / power(10,itokens.underlying_decimals) AS redeem_amount,
redeemAmount / power(10,itokens.underlying_decimals)*p.price AS redeem_usd
FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_Redeem') }} redeem
LEFT JOIN {{ ref('ironbank_optimism_itokens') }} itokens ON redeem.contract_address = itokens.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', redeem.evt_block_time) AND p.contract_address = itokens.underlying_token_address
WHERE p.blockchain = 'optimism'