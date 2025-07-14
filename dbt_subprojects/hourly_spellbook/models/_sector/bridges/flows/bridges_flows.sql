{{ config(
    schema = 'bridges',
    alias = 'flows',
    materialized = 'view'
    )
}}


{% set vms = [
    'evms'
] %}

SELECT *
    FROM (
        {% for vm in vms %}
        SELECT deposit_chain
        , withdrawal_chain
        , bridge_name
        , bridge_version
        , deposit_block_date
        , deposit_block_time
        , deposit_block_number
        , withdraw_block_date
        , withdraw_block_time
        , withdraw_block_number
        , deposit_amount_raw
        , deposit_amount
        , withdrawal_amount_raw
        , withdrawal_amount
        , amount_usd
        , sender
        , recipient
        , deposit_token_standard
        , withdrawal_token_standard
        , deposit_token_address
        , withdrawal_token_address
        , deposit_tx_from
        , deposit_tx_hash
        , withdraw_tx_hash
        , bridge_transfer_id
        FROM {{ ref('bridges_'~vm~'_deposits') }}
        {% if is_incremental() %}
        WHERE  {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )