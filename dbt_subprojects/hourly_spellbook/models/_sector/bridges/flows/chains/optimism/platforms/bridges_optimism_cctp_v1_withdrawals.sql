{% set blockchain = 'optimism' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'cctp_v1_withdrawals',
    materialized = 'view',
    )
}}

{{cctp_v1_withdrawals(blockchain = blockchain)}}