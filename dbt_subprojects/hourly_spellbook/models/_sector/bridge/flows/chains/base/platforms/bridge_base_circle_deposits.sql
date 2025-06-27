{% set blockchain = 'base' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'circle_deposits',
    materialized = 'view',
    )
}}

SELECT 'base' AS deposit_chain
, i.blockchain AS withdraw_chain
, 'Circle' AS project
, '2' AS project_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, amount AS withdraw_amount_raw
, depositor
, varbinary_substring(mintRecipient,13) AS recipient
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdraw_token_standard
, burnToken AS deposit_token_address
, NULL AS withdraw_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(["Base Bridge'" "1", 'deposit_chain', 'withdraw_chain', '"from"', 'to', 'localtoken', 'remotetoken', 'amount', 'amount'])}} as bridge_id
FROM {{ source('circle_base', 'tokenmessengerv2_evt_depositforburn')}}