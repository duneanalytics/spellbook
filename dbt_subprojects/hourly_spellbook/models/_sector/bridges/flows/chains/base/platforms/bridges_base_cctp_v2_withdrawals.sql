{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'cctp_v2_withdrawals',
    materialized = 'view',
    )
}}

{{cctp_v2_withdrawals(blockchain = blockchain)}}