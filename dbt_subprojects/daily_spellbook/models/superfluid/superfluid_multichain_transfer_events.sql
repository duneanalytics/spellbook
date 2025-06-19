{{ config(
    schema = 'superfluid_multichain',
    alias = 'transfer_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','index'],
    )
}}

SELECT
    blockchain,
    evt_block_time AS block_time,
    evt_block_date AS block_date,
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    contract_address,
    "from",
    "to",
    "value"
FROM {{ ref('evms_erc20_transfers') }} transfers
WHERE (blockchain, contract_address) IN (
    SELECT blockchain, token_address AS contract_address FROM {{ ref('superfluid_multichain_supertoken_addresses') }}
)