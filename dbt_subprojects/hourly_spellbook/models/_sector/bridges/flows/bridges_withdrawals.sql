{{ config(
    schema = 'bridges'
    , alias = 'withdrawals'
    , materialized = 'view'
)
}}

{% set vms = [
    'evms'
] %}

WITH grouped_withdrawals AS (
SELECT *
FROM (
        {% for vm in vms %}
        SELECT deposit_chain
        , withdrawal_chain
        , bridge_name
        , bridge_version
        , block_date
        , block_time
        , block_number
        , withdrawal_amount_raw
        , sender
        , recipient
        , withdrawal_token_standard
        , withdrawal_token_address
        , tx_from
        , tx_hash
        , evt_index
        , contract_address
        , bridge_transfer_id
        FROM {{ ref('bridges_'~vm~'_withdrawals') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )