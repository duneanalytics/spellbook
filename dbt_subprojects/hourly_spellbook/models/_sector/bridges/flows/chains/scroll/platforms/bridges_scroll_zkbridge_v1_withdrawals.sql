{% set blockchain = 'scroll' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'zkbridge_v1_withdrawals',
    materialized = 'view',
    )
}}

{{zkbridge_v1_withdrawals(blockchain = blockchain)}}