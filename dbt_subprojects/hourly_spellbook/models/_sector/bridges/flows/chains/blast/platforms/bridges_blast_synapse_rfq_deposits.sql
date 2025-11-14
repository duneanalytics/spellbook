{% set blockchain = 'blast' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'synapse_rfq_deposits',
    materialized = 'view',
    )
}}

{{synapse_rfq_deposits(
    blockchain = blockchain
    )}}