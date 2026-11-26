{{ config(
    schema = 'bridges_arbitrum'
    , alias = 'aero_v1_deposits'
    , materialized = 'view'
    , filtering_columns = ['block_date']
) }}

SELECT deposit_chain
    , withdrawal_chain_id
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , block_date
    , block_time
    , block_number
    , deposit_amount_raw
    , sender
    , recipient
    , deposit_token_address
    , deposit_token_standard
    , tx_from
    , tx_hash
    , evt_index
    , contract_address
    , bridge_transfer_id
FROM {{ ref('bridges_aero_v1_deposits') }}
WHERE deposit_chain = 'arbitrum'
