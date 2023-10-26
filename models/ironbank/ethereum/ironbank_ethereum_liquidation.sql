{{ config(
    
    alias = 'liquidation',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
l.evt_block_number AS block_number,
l.evt_block_time AS block_time,
l.evt_tx_hash AS tx_hash,
l.evt_index AS index,
l.contract_address AS contract_address,
l.borrower,
i.symbol,
i.underlying_symbol,
i_asset.underlying_token_address AS underlying_address,
i_collateral.underlying_token_address AS collateral_address,
CAST(l.repayAmount AS DOUBLE) / power(10,i.underlying_decimals) AS repay_amount,
CAST(l.repayAmount AS DOUBLE) / power(10,i.underlying_decimals)*p.price AS repay_usd
FROM {{ source('ironbank_ethereum', 'CErc20Delegator_evt_LiquidateBorrow') }} l
LEFT JOIN (SELECT contract_address,underlying_token_address
            FROM {{ ref('ironbank_ethereum_itokens') }} ) i_collateral ON l.cTokenCollateral = i_collateral.contract_address
LEFT JOIN (SELECT contract_address,underlying_token_address
            FROM {{ ref('ironbank_ethereum_itokens') }} ) i_asset ON l.contract_address = i_asset.contract_address
LEFT JOIN {{ ref('ironbank_ethereum_itokens') }} i ON l.contract_address = i.contract_address
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', l.evt_block_time) AND p.contract_address = i.underlying_token_address AND p.blockchain = 'ethereum'