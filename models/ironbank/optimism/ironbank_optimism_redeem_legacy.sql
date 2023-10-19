{{ config(
	tags=['legacy'],
	
    alias = alias('redeem', legacy_model=True),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "ironbank",
                                \'["michael-ironbank"]\') }}'
) }}

SELECT
r.evt_block_number AS block_number,
r.evt_block_time AS block_time,
r.evt_tx_hash AS tx_hash,
r.evt_index AS `index`,
CAST(r.contract_address AS VARCHAR(100)) AS contract_address,
r.redeemer,
i.symbol,
i.underlying_symbol,
i.underlying_token_address AS underlying_address,
CAST(r.redeemAmount AS DOUBLE) / power(10,i.underlying_decimals) AS redeem_amount,
CAST(r.redeemAmount AS DOUBLE) / power(10,i.underlying_decimals)*p.price AS redeem_usd
FROM {{ source('ironbank_optimism', 'CErc20Delegator_evt_Redeem') }} r
LEFT JOIN {{ ref('ironbank_optimism_itokens_legacy') }} i ON CAST(r.contract_address AS VARCHAR(100)) = i.contract_address
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', r.evt_block_time) AND CAST(p.contract_address AS VARCHAR(100)) = i.underlying_token_address AND p.blockchain = 'optimism'