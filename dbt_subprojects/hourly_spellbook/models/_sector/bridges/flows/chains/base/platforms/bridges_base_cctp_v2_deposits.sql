{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'cctp_v2_deposits',
    materialized = 'view',
    )
}}

{{cctp_v2_deposits(blockchain = blockchain)}}