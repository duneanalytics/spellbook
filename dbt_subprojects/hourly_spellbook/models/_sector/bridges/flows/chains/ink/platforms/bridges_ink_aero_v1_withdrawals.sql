{{ config(
    schema = 'bridges_ink'
    , alias = 'aero_v1_withdrawals'
    , materialized = 'view'
    , filtering_columns = ['block_date']
) }}

SELECT deposit_chain_id
    , deposit_chain
    , withdrawal_chain
    , bridge_name
    , bridge_version
    , block_date
    , block_time
    , block_number
    , withdrawal_amount_raw
    , sender
    , recipient
    , withdrawal_token_address
    , withdrawal_token_standard
    , tx_from
    , tx_hash
    , evt_index
    , contract_address
    , bridge_transfer_id
FROM {{ ref('bridges_aero_v1_withdrawals') }}
WHERE withdrawal_chain = 'ink'
