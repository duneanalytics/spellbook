{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'lighter_v1_withdrawals',
    materialized = 'view',
    )
}}

SELECT CAST(NULL AS DOUBLE) AS deposit_chain_id
, 'lighter' AS deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'Lighter' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, usdcAmount AS withdrawal_amount_raw
, CAST(NULL AS VARBINARY) AS sender
, CAST(NULL AS VARBINARY) AS recipient
, 'erc20' AS withdrawal_token_standard
, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('lighter_v2_ethereum', 'zklighter_evt_withdraw') }} d