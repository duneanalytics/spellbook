{{ config(
    alias = 'liquidation',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
'ironbank' AS project,
'1' AS version,
evt_block_number AS block_number,
evt_block_time AS block_time,
evt_tx_hash AS tx_hash,
evt_index,
borrower AS liquidated_borrower,
i_asset.asset_underlying_token_address AS debt_to_cover_asset_address,
i_collateral.collateral_underlying_token_address AS collateral_asset_address,
repayAmount AS debt_to_cover_amount
FROM (
    SELECT * FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_LiquidateBorrow') }}
) i
LEFT JOIN (SELECT contract_address as collateral_contract_address, 
                    underlying_token_address as collateral_underlying_token_address
            FROM {{ ref('ironbank_optimism_itokens') }} ) i_collateral ON i.cTokenCollateral = i_collateral.collateral_contract_address
LEFT JOIN (SELECT contract_address as asset_contract_address, 
                    underlying_token_address as asset_underlying_token_address
            FROM {{ ref('ironbank_optimism_itokens') }} ) i_asset ON i.contract_address = i_asset.asset_contract_address