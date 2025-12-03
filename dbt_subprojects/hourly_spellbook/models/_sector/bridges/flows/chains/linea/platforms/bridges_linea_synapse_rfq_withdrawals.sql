{% set blockchain = 'linea' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'synapse_rfq_withdrawals',
    materialized = 'view',
    )
}}

{{synapse_rfq_withdrawals(
    blockchain = blockchain
    )}}