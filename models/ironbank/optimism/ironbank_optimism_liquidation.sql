{{ config(
    alias = 'liquidation',
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
i_asset.asset_underlying_token_address AS underlying_address,
i_collateral.collateral_underlying_token_address AS collateral_address,
repayAmount / power(10,itokens.underlying_decimals) AS repay_amount,
repayAmount / power(10,itokens.underlying_decimals)*p.price AS repay_usd
FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_LiquidateBorrow') }} liquidation
LEFT JOIN (SELECT contract_address as collateral_contract_address, 
                    underlying_token_address as collateral_underlying_token_address
            FROM {{ ref('ironbank_optimism_itokens') }} ) i_collateral ON liquidation.cTokenCollateral = i_collateral.collateral_contract_address
LEFT JOIN (SELECT contract_address as asset_contract_address, 
                    underlying_token_address as asset_underlying_token_address
            FROM {{ ref('ironbank_optimism_itokens') }} ) i_asset ON liquidation.contract_address = i_asset.asset_contract_address
LEFT JOIN {{ ref('ironbank_optimism_itokens') }} itokens ON liquidation.contract_address = itokens.contract_address
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', liquidation.evt_block_time) AND p.contract_address = itokens.underlying_token_address
WHERE p.blockchain = 'optimism'