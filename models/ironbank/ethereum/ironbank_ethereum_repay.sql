{{ config(
    alias = 'repay',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
'IronBank' AS project,
'1' AS version,
evt_block_number AS block_number,
evt_block_time AS block_time,
evt_tx_hash AS tx_hash,
evt_index,
NULL::integer[] AS trace_address,
borrower,
i."underlying_token_address" AS asset_address,
"repayAmount" AS repay_amount
FROM (
SELECT * FROM {{ source('ironbank_ethereum', 'CErc20Delegator_evt_RepayBorrow') }}
) ironbank
LEFT JOIN ironbank_ethereum.itokens i ON ironbank.contract_address = i.contract_address