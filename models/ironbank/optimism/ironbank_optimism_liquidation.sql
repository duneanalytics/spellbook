{{ config(
    alias = alias('liquidation'),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
l.evt_block_number AS block_number,
l.evt_block_time AS block_time,
l.evt_tx_hash AS tx_hash,
l.evt_index AS `index`,
CAST(l.contract_address AS VARCHAR(100)) AS contract_address,
l.borrower,
i.symbol,
i.underlying_symbol,
i_asset.underlying_token_address AS underlying_address,
i_collateral.underlying_token_address AS collateral_address,
CAST(l.repayAmount AS DOUBLE) / power(10,i.underlying_decimals) AS repay_amount,
CAST(l.repayAmount AS DOUBLE) / power(10,i.underlying_decimals)*p.price AS repay_usd
FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_LiquidateBorrow') }} l
LEFT JOIN (SELECT contract_address,underlying_token_address
            FROM {{ ref('ironbank_optimism_itokens') }} ) i_collateral ON CAST(l.cTokenCollateral AS VARCHAR(100)) = i_collateral.contract_address
LEFT JOIN (SELECT contract_address,underlying_token_address
            FROM {{ ref('ironbank_optimism_itokens') }} ) i_asset ON CAST(l.contract_address AS VARCHAR(100)) = i_asset.contract_address
LEFT JOIN {{ ref('ironbank_optimism_itokens') }} i ON CAST(l.contract_address AS VARCHAR(100)) = i.contract_address
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time) AND CAST(p.contract_address AS VARCHAR(100)) = i.underlying_token_address AND p.blockchain = 'optimism'