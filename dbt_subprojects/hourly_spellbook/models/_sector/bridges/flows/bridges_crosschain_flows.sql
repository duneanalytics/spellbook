{{ config(
    schema = 'bridges_crosschain',
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
        , CAST(sender AS VARCHAR) AS sender
        , CAST(recipient AS VARCHAR) AS recipient
        , deposit_token_standard
        , withdrawal_token_standard
        , CAST(deposit_token_address AS VARCHAR) AS deposit_token_address
        , CAST(withdrawal_token_address AS VARCHAR) AS withdrawal_token_address
        , CAST(deposit_tx_from AS VARCHAR) AS deposit_tx_from
        , CAST(deposit_tx_hash AS VARCHAR) AS deposit_tx_hash
        , CAST(withdraw_tx_hash AS VARCHAR) AS withdraw_tx_hash
        , bridge_transfer_id
        FROM {{ ref('bridges_'~vm~'_flows') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %} 
        )