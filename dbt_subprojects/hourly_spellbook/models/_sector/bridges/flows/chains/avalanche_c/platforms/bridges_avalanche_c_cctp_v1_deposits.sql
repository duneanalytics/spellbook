{% set blockchain = 'avalanche_c' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'cctp_v1_deposits',
    materialized = 'view',
    )
}}

{{cctp_v1_deposits(blockchain = blockchain)}}